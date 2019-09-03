open Prelude

module Fpass = Cd.Fpass(Uid)
module Verify = Cd.Verify(Uid)(Us)(IO)

let z = Dd.bigstring_create Dd.io_buffer_size
let allocate bits = Dd.make_window ~bits

let zip a b =
  if Array.length a <> Array.length b then Fmt.invalid_arg "Array.zip" ;
  Array.init (Array.length a) (fun i -> a.(i), b.(i))

(* Verbose mode *)

let verbose = ref false

let succ_indexation, end_indexation, max_indexation =
  let max_indexation = ref 0 in
  let cur_indexation = ref 0 in
  (fun () -> incr cur_indexation
           ; if !verbose
             then Fmt.pr "\robjects: %3d%% (%d/%d)%!"
                 (!cur_indexation * 100 / !max_indexation)
                 !cur_indexation !max_indexation),
  (fun () -> if !verbose then Fmt.pr ", done.\n%!"),
  max_indexation

let mutex = Mutex.create ()

let succ_delta, end_delta, max_delta =
  let max_delta = ref 0 in
  let cur_delta = ref 0 in
  (fun () -> incr cur_delta
           ; if !verbose
             then ( Mutex.lock mutex
                  ; Fmt.pr "\rdeltas:  %3d%% (%d/%d)%!"
                      (!cur_delta * 100 / !max_delta)
                      !cur_delta !max_delta
                  ; Mutex.unlock mutex )),
  (fun () -> if !verbose then ( Mutex.lock mutex ; Fmt.pr ", done.\n%!" ; Mutex.unlock mutex )),
  max_delta

(* End of verbose mode *)

let first_pass ~digest fpath =
  let ic = open_in (Fpath.to_string fpath) in
  let zw = Dd.make_window ~bits:15 in
  let allocate _ = zw in

  let (_, max) = Us.prj (Fpass.check_header unix unix_read ic) in
  seek_in ic 0 ;
  max_indexation := max ;

  let decoder = Fpass.decoder ~o:z ~allocate (`Channel ic) in

  let children = Hashtbl.create 0x100 in
  let where = Hashtbl.create max in
  let weight = Hashtbl.create max in
  let checks = Hashtbl.create max in
  let matrix = Array.make max Verify.unresolved_node in

  let rec go decoder = match Fpass.decode decoder with
    | `Await _ | `Peek _ -> assert false
    | `Entry ({ Fpass.kind= Base _
              ; offset; size; crc; _ }, decoder) ->
      let n = Fpass.count decoder - 1 in
      Hashtbl.add weight offset size ;
      Hashtbl.add checks offset crc ;
      Hashtbl.add where offset n ;
      matrix.(n) <- Verify.unresolved_base ~cursor:offset ;
      succ_indexation () ;
      incr max_delta ;
      go decoder
    | `Entry ({ Fpass.kind= Ofs { sub; source; target; }
              ; offset; crc; _ }, decoder) ->
      let n = Fpass.count decoder - 1 in
      Hashtbl.add weight (offset - sub) source ;
      Hashtbl.add weight offset target ;
      Hashtbl.add checks offset crc ;
      Hashtbl.add where offset n ;

      succ_indexation () ;
      incr max_delta ;

      ( try let v = Hashtbl.find children (`Ofs (offset - sub)) in
          Hashtbl.add children (`Ofs (offset - sub)) (offset :: v)
        with Not_found ->
          Hashtbl.add children (`Ofs (offset - sub)) [ offset ] ) ;
      go decoder
    | `Entry _ -> assert false (* OBJ_REF *)
    | `End uid -> end_indexation () ; close_in ic ; Ok uid
    | `Malformed _ as err -> Error err in
  match go decoder with
  | Error _ as err -> err
  | Ok uid ->
    Ok ({ Cd.where= (fun ~cursor -> Hashtbl.find where cursor)
        ; children= (fun ~cursor ~uid ->
              match Hashtbl.find_opt children (`Ofs cursor),
                    Hashtbl.find_opt children (`Ref uid) with
              | Some a, Some b -> List.sort_uniq (compare : int -> int -> int) (a @ b)
              | Some x, None | None, Some x -> x
              | None, None -> [])
        ; digest= (fun ~kind ?off ?len buf -> succ_delta () ; digest ~kind ?off ?len buf)
        ; weight= (fun ~cursor -> Hashtbl.find weight cursor) },
        matrix, where, checks, uid)

exception Invalid_pack

module Idx = Cd.Idx.N(Uid)

let index ~digest v output fpath =
  verbose := v ;

  let oc, oc_close = match output with
    | None ->
      let fpath = Fpath.set_ext "idx" fpath in
      let oc = open_out (Fpath.to_string fpath) in
      oc, (fun () -> close_out oc)
    | Some fpath ->
      let oc = open_out (Fpath.to_string fpath) in
      oc, (fun () -> close_out oc) in
  let open Rresult.R in
  first_pass ~digest fpath >>= fun (oracle, matrix, where, checks, pack) ->
  let fd = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY ] 0o644 in
  let mx =
    let ic = Unix.in_channel_of_descr fd in
    in_channel_length ic in
  let index _ = raise Not_found in
  let t = Cd.make { fd; mx; } ~z ~allocate ~uid_ln:Uid.length ~uid_rw:Uid.of_raw_string index in

  Verify.verify ~map:unix_map ~oracle t ~matrix ;
  end_delta () ;

  let offsets =
    Hashtbl.fold (fun k _ a -> k :: a) where []
    |> List.sort (Stdlib.compare : int -> int -> int)
    |> Array.of_list in
  let matrix = zip offsets matrix in
  let entries =
    Array.map (fun (offset, s) ->
        let uid = Verify.uid_of_status s in
        let crc = Hashtbl.find checks offset in
        { Idx.crc; Idx.offset; Idx.uid }) matrix in
  let encoder = Idx.encoder (`Channel oc) ~pack entries in
  let go () = match Idx.encode encoder `Await with `Partial -> assert false | `Ok -> () in
  go () ; oc_close () ; Fmt.pr "%a\n%!" Uid.pp pack ; Ok ()

open Cmdliner

let existing_fpath ~ext =
  let parser x = match Fpath.of_string x with
    | Ok v when Sys.file_exists x ->
      if Fpath.has_ext ext v then Ok v else Rresult.R.error_msgf "%a has a bad extension" Fpath.pp v
    | Ok v -> Rresult.R.error_msgf "%a does not exist" Fpath.pp v
    | Error _ as err -> err in
  let pp = Fpath.pp in
  Arg.conv (parser, pp)

let fpath ~ext =
  let parser x = match Fpath.of_string x with
    | Ok v ->
      if Fpath.has_ext ext v then Ok v else Rresult.R.error_msgf "%a has a bad extension" Fpath.pp v
    | Error _ as err -> err in
  let pp = Fpath.pp in
  Arg.conv (parser, pp)

let pack =
  let doc = "The pack archive." in
  Arg.(required & pos 0 ~rev:true (some (existing_fpath ~ext:"pack")) None & info [] ~docv:"<pack>.pack" ~doc)

let output =
  let doc = "Write the generated pack indx into the specified file. Without this option, the name of pack index \
             file is constructed from the name of packed archive file by replacing .pack with .idx \
             (and the program fails if the name of packed archive does not end with .pack)." in
  Arg.(value & opt (some (fpath ~ext:"idx")) None & info [ "o"; "output" ] ~docv:"<index-file>" ~doc)

let verbose =
  let doc = "After verifying the pack, show list of objects contained in the pack." in
  Arg.(value & flag & info [ "v"; "verbose" ] ~doc)

let cmd ~digest =
  let doc = "Validate packed archive files" in
  let exits = Term.default_exits in
  let man =
    [ `S Manpage.s_description
    ; `P "Reads a packed archive (.pack) from the specified file, and builds a pack index file (.idx) \
          for it." ] in
  Term.(const (index ~digest) $ verbose $ output $ pack),
  Term.info "index" ~doc ~exits ~man

