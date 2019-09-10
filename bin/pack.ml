open Prelude
open Core

let collect ~root fformat =
  let uid_wr ~cursor x =
    if String.length x - cursor >= (Uid.length * 2)
    then cursor + (Uid.length * 2), Uid.of_hex (String.sub x cursor (Uid.length * 2))
    else raise (Invalid_argument "uid_wr") in
  let entries = Hashtbl.create 0x100 in
  Bos.OS.Dir.fold_contents
    ~elements:`Files
    ~traverse:`Any
    (fun fpath () ->
       let[@warning "-8"] Some v = Fpath.rem_prefix root fpath in
       match Fformat.scan ~uid_wr fformat (Fpath.to_string v) with
       | Some (uid, kind) ->
         Hashtbl.add entries uid (fpath, kind)
       | None -> ())
    () root |> function
  | Ok () -> Ok entries
  | Error _ as err -> err

let metadata ~uid ~kind fpath =
  let ic = open_in (Fpath.to_string fpath) in
  let ln = in_channel_length ic in
  let name = match kind with
    | `C -> Some (Fpath.filename fpath)
    | `A | `B | `D -> None in
  close_in ic ; Carton.Enc.make_entry ~kind ~length:ln ?name uid

let load entries uid = match Hashtbl.find entries uid with
  | (fpath, kind) ->
    let st = Unix.stat (Fpath.to_string fpath) in
    let fd = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY] 0o644 in
    let mp0 = Mmap.V1.map_file fd Bigarray.char Bigarray.c_layout false [| st.Unix.st_size |] in
    let mp1 = Bigarray.array1_of_genarray mp0 in
    Gc.finalise (fun _ -> Unix.close fd) mp0 ;
    Us.inj (Carton.Dec.v ~kind ~depth:0 mp1)

module Verbose = struct
  type 'a fiber = 'a

  let cur_entries = ref 0
  let max_entries = ref 0
  let verbose_entries = ref false

  let succ () = incr cur_entries
  let print () = if !verbose_entries then Fmt.pr "\rdeltify: %d%% (%d/%d)%!" (!cur_entries * 100 / !max_entries) !cur_entries !max_entries
  let flush () = if !verbose_entries then Fmt.pr "\n%!"
end

module D = Carton.Enc.Delta(Us)(IO)(Uid)(Verbose)

let deltify ~digest:_ ?(threads= 4) ~root fformat =
  let open Rresult.R in
  collect ~root fformat >>= fun entries ->
  let load = load entries in
  let entries0 = Hashtbl.fold (fun k v a -> (k, v) :: a) entries [] |> Array.of_list in
  let entries1 = Array.map (fun (uid, (fpath, kind)) -> metadata ~uid ~kind fpath) entries0 in
  Array.stable_sort Carton.Enc.compare_entry entries1 ;
  Verbose.max_entries := Array.length entries1 ;
  let targets = D.delta ~threads:(List.init threads (fun _ -> load)) ~weight:0x1000 entries1 in
  Verbose.flush () ; Ok (entries, targets)

(* Verbose pack *)

let verbose_targets = ref false

let max_targets, succ_targets, print_targets, end_targets =
  let cur_targets = ref 0 in
  let max_targets = ref 0 in
  max_targets, (fun () -> incr cur_targets),
  (fun () -> if !verbose_targets then Fmt.pr "\robjects emitted: %d%% (%d/%d)%!" (!cur_targets * 100 / !max_targets) !cur_targets !max_targets),
  (fun () -> if !verbose_targets then Fmt.pr "\n%!")

(* Verbose pack *)

let header = Bigstringaf.create 12
let ctx = ref Uid.empty

let output_bigstring oc buf ~off ~len =
  ctx := Uid.feed !ctx buf ~off ~len ;
  let s = Bigstringaf.substring buf ~off ~len in
  output_string oc s

let pack ~digest ?threads ~root fformat output =
  let oc, oc_close = match output with
    | `Stdout -> stdout, (fun () -> ())
    | `Fpath v ->
      let oc = open_out (Fpath.to_string v) in
      oc, (fun () -> close_out oc) in

  let open Rresult.R in
  deltify ~digest ?threads ~root fformat >>= fun (entries, targets) ->
  let offsets = Hashtbl.create (Array.length targets) in

  let find uid = match Hashtbl.find offsets uid with
    | v -> Us.inj (Some v)
    | exception Not_found -> Us.inj None in

  let uid =
    { Carton.Enc.uid_ln= Uid.length
    ; Carton.Enc.uid_rw= Uid.to_raw_string } in

  let load = load entries in

  let b =
    { Carton.Enc.o= Bigstringaf.create Dd.io_buffer_size
    ; Carton.Enc.i= Bigstringaf.create Dd.io_buffer_size
    ; Carton.Enc.q= Dd.B.create 0x10000
    ; Carton.Enc.w= Dd.make_window ~bits:15 } in

  max_targets := Array.length targets ;
  Carton.Enc.header_of_pack ~length:(Array.length targets) header 0 12 ;
  output_bigstring oc header ~off:0 ~len:12 ;

  let iter targets =
    let cursor = ref 12 (* XXX(dinosaure): header. *) in

    for i = 0 to Array.length targets - 1
    do
      Hashtbl.add offsets (Carton.Enc.target_uid targets.(i)) !cursor ;

      let fiber () =
        let ( >>= ) = unix.bind in

        Carton.Enc.encode_target unix
          ~b ~find ~load ~uid targets.(i) ~cursor:!cursor >>= fun (len, encoder) ->
        let rec go encoder = match Carton.Enc.N.encode ~o:b.o encoder with
          | `Flush (encoder, len) ->
            output_bigstring oc b.o ~off:0 ~len ;
            cursor := !cursor + len ;
            let encoder = Carton.Enc.N.dst encoder b.o 0 (Bigstringaf.length b.o) in
            go encoder
          | `End -> () in
        output_bigstring oc b.o ~off:0 ~len ;
        cursor := !cursor + len ;
        let encoder = Carton.Enc.N.dst encoder b.o 0 (Bigstringaf.length b.o) in
        go encoder ; Us.inj () in
      Us.prj (fiber ()) ; succ_targets () ; print_targets ()
    done in

  iter targets ;
  end_targets () ;
  output_string oc (Uid.to_raw_string (Uid.get !ctx)) ;
  oc_close () ;
  Ok ()

let pack ~digest threads verbose root fformat output =
  if verbose then ( verbose_targets := true ; Verbose.verbose_entries := true ) ;
  pack ~digest ~threads ~root fformat output

open Cmdliner

let fformat =
  let parser = Fformat.parse in
  let pp = Fformat.pp in
  Arg.conv (parser, pp)

let number ~default =
  let parser x =
    try let x = int_of_string x in if x <= 0 then Ok default else Ok x
    with _ -> Rresult.R.error_msgf "Invalid number: %S" x in
  let pp = Fmt.int in
  Arg.conv (parser, pp)

let existing_directory =
  let parser x = match Fpath.of_string x with
    | Ok v -> if Sys.is_directory x then Ok v else Rresult.R.error_msgf "%a is not a directory" Fpath.pp v
    | Error _ as err -> err in
  let pp = Fpath.pp in
  Arg.conv (parser, pp)

let fpath =
  let parser = function
    | "-" -> Ok `Stdout
    | x -> match Fpath.of_string x with
      | Ok v -> Ok (`Fpath v)
      | Error _ as err -> err in
  let pp ppf = function
    | `Stdout -> Fmt.string ppf "-"
    | `Fpath v -> Fpath.pp ppf v in
  Arg.conv (parser, pp)

let threads =
  let doc = "Specifies the number of threads to spawn when resolving deltas. \
             This is meant to reduce packing time on multiprocessor machines. \
             The required amount of memory for the delta search window is however \
             multiplied by the number of threads." in
    Arg.(value & opt (number ~default:cpu) cpu & info [ "threads" ] ~doc ~docv:"<n>")

let fformat =
  let doc = "Format of path to store objects." in
  Arg.(required & opt (some fformat) None & info [ "f"; "format" ] ~doc ~docv:"<format>")

let verbose =
  let doc = "Say where objects are stored." in
  Arg.(value & flag & info [ "v"; "verbose" ] ~doc)

let root =
  let doc = "" in
  Arg.(required & opt (some existing_directory) None & info [ "r"; "root" ] ~doc ~docv:"<dir>")

let output =
  let doc = "" in
  Arg.(value & pos ~rev:true 0 fpath `Stdout & info [] ~doc ~docv:"<output>")

let cmd ~digest =
  let doc = "Pack objects to a packed archive." in
  let exits = Term.default_exits in
  let man =
    [ `S Manpage.s_description
    ; `P "Pack objects to a packed archive." ] in
  Term.(const (pack ~digest) $ threads $ verbose $ root $ fformat $ output),
  Term.info "pack" ~doc ~exits ~man
