open Prelude
open Core

module Like_git = struct
  type t = (Uid.t, string) Hashtbl.t

  let parser_tree =
    let open Angstrom in
    let entry =
      take_while1 ((<>) ' ') *>
      char ' ' *> take_while1 ((<>) '\000') >>= fun name ->
      char '\000' *> take Digestif.SHA1.digest_size >>| Digestif.SHA1.of_raw_string >>= fun uid ->
      return (name, uid) in
    many entry

  let collect t ~uid ~kind fpath =
    match kind with
    | `A | `C | `D -> ()
    | `B (* tree *) ->
      let ic = open_in (Fpath.to_string fpath) in
      let ln = in_channel_length ic in
      let rs = Bytes.create ln in
      really_input ic rs 0 ln ; close_in ic ;
      match Angstrom.parse_string parser_tree (Bytes.unsafe_to_string rs) with
      | Ok rs -> List.iter (fun (name, uid) -> Hashtbl.add t uid name) rs
      | Error _ -> Fmt.failwith "%a is not a valid tree object" Uid.pp uid
end

let collect ?(like_git= false) ~root fformat =
  let uid_wr ~cursor x =
    if String.length x - cursor >= (Uid.length * 2)
    then cursor + (Uid.length * 2), Uid.of_hex (String.sub x cursor (Uid.length * 2))
    else raise (Invalid_argument "uid_wr") in
  let entries = Hashtbl.create 0x100 in
  let names = Hashtbl.create 0x100 in
  Bos.OS.Dir.fold_contents
    ~elements:`Files
    ~traverse:`Any
    (fun fpath () ->
       let[@warning "-8"] Some v = Fpath.rem_prefix root fpath in
       match Fformat.scan ~uid_wr fformat (Fpath.to_string v) with
       | Some (uid, kind) ->
         if like_git then Like_git.collect names ~uid ~kind fpath ;
         Hashtbl.add entries uid (fpath, kind)
       | None -> ())
    () root |> function
  | Ok () -> Ok (entries, names)
  | Error _ as err -> err

type metadata_like_git =
  | Commit
  | Tree of string option
  | Blob of string option * int
  | Tag

let metadata_like_git names ~uid ~kind fpath =
  let v = match kind with
    | `A -> Commit
    | `B -> Tree (Hashtbl.find_opt names uid)
    | `C ->
      let size =
        let open Rresult.R in
        Bos.OS.Path.stat fpath
        >>| (fun st -> st.Unix.st_size)
        |> function Ok v -> v | Error _ -> 0 in
      Blob (Hashtbl.find_opt names uid, size)
    | `D -> Tag in
  let ln =
    let ic = open_in (Fpath.to_string fpath) in
    let ln = in_channel_length ic in close_in ic ; ln in
  Carton.Enc.make_entry ~kind ~length:ln uid v

let compare_like_git a b =
  let inf = (-1) and sup = 1 in
  match a, b with
  | Commit, Commit -> 0
  | Commit, (Tree _ | Blob _ | Tag) -> inf
  | (Tree _ | Blob _ | Tag), Commit -> sup
  | Tree None, Tree None -> 0
  | Tree None, Tree (Some _) -> inf
  | Tree (Some _), Tree None -> sup
  | Tree (Some n0), Tree (Some n1) ->
    String.compare n0 n1
  | Tree _, (Blob _ | Tag) -> inf
  | (Blob _ | Tag), Tree _ -> sup
  | Blob (Some n0, s0), Blob (Some n1, s1) ->
    if String.equal n0 n1
    then Stdlib.compare s1 s0
    else String.compare n0 n1
  | Blob (_, s0), Blob (_, s1) -> Stdlib.compare s1 s0
  | Blob _, Tag -> inf
  | Tag, Blob _ -> sup
  | Tag, Tag -> 0

type metadata = int

let metadata ~uid ~kind fpath =
  let v =
    let open Rresult.R in
    Bos.OS.Path.stat fpath
    >>| (fun st -> st.Unix.st_size)
    |> function Ok v -> v | Error _ -> 0 in
  Carton.Enc.make_entry ~kind ~length:v uid v

let load entries uid = match Hashtbl.find entries uid with
  | (fpath, kind) ->
    let st = Unix.stat (Fpath.to_string fpath) in
    let fd = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY] 0o644 in
    let mp0 = Mmap.V1.map_file fd Bigarray.char Bigarray.c_layout false [| st.Unix.st_size |] in
    let mp1 = Bigarray.array1_of_genarray mp0 in
    Gc.finalise (fun _ -> Unix.close fd) mp1 ;
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

module type VALUE = sig
  type t

  val hash : t -> int
  val equal : t -> t -> bool
end

module Value_like_git = struct
  type t = metadata_like_git

  let pp ppf = function
    | Commit -> Fmt.string ppf "commit"
    | Tree (Some name) -> Fmt.pf ppf "<tree:%s>" name
    | Tree None -> Fmt.string ppf "<root>"
    | Blob (Some name, size) -> Fmt.pf ppf "<blob:%s:%d>" name size
    | Blob (None, size) -> Fmt.pf ppf "<blob:unknow:%d>" size
    | Tag -> Fmt.string ppf "tag"

  let hash = Hashtbl.hash
  let equal a b = compare_like_git a b = 0
end

module Value = struct
  type t = metadata

  let pp = Fmt.int
  let hash = Hashtbl.hash
  let equal a b = a = b
end

type 'uid w = W : (module VALUE with type t = 'a) * (Uid.t, 'a) Carton.Enc.entry array -> 'uid w
type 'uid t = V : (Uid.t, (Fpath.t * [ `A | `B | `C | `D ])) Hashtbl.t
                  * (module VALUE with type t = 'a)
                  * ('uid, 'a) Carton.Enc.q array -> 'uid t

let deltify ~digest:_ ?(like_git= false) ?(threads= 4) ~root fformat =
  let open Rresult.R in
  collect ~like_git ~root fformat >>= fun (entries, names) ->
  let load = load entries in
  let entries0 = Hashtbl.fold (fun k v a -> (k, v) :: a) entries [] |> Array.of_list in
  let W ((module V), entries1) =
    if like_git
    then
      ( let res = Array.map (fun (uid, (fpath, kind)) -> metadata_like_git names ~uid ~kind fpath) entries0 in
        Array.sort (fun a b -> compare_like_git (Carton.Enc.value a) (Carton.Enc.value b)) res
      ; W ((module Value_like_git), res) )
    else
      ( let res = Array.map (fun (uid, (fpath, kind)) -> metadata ~uid ~kind fpath) entries0 in
        Array.sort (fun a b -> Stdlib.compare (Carton.Enc.value a) (Carton.Enc.value b)) res
      ; W ((module Value), res) ) in
  Verbose.max_entries := Array.length entries1 ;
  let module D = Carton.Enc.Delta(Us)(IO)(Uid)(Verbose) in
  let targets = D.delta ~threads:(List.init threads (fun _ -> load)) ~weight:0x1000 ~uid_ln:Uid.length entries1 in
  Verbose.flush () ; return (V (entries, (module V), targets))

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

let pack ~digest ?threads ~root fformat output like_git =
  let oc, oc_close = match output with
    | `Stdout -> stdout, (fun () -> ())
    | `Fpath v ->
      let oc = open_out (Fpath.to_string v) in
      oc, (fun () -> close_out oc) in

  let open Rresult.R in
  deltify ~digest ~like_git:like_git ?threads ~root fformat >>= fun (V (entries, _, targets)) ->
  let offsets = Hashtbl.create (Array.length targets) in

  let find uid = match Hashtbl.find offsets uid with
    | v -> Us.inj (Some v)
    | exception Not_found -> Us.inj None in

  let uid =
    { Carton.Enc.uid_ln= Uid.length
    ; Carton.Enc.uid_rw= Uid.to_raw_string } in

  let load = load entries in

  let b =
    { Carton.Enc.o= Bigstringaf.create De.io_buffer_size
    ; Carton.Enc.i= Bigstringaf.create De.io_buffer_size
    ; Carton.Enc.q= De.Queue.create 0x10000
    ; Carton.Enc.w= De.make_window ~bits:15 } in

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

let pack ~digest threads verbose root fformat output like_git =
  if verbose then ( verbose_targets := true ; Verbose.verbose_entries := true ) ;
  pack ~digest ~threads ~root fformat output like_git

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
  let doc = "Format of path of stored objects." in
  Arg.(required & opt (some fformat) None & info [ "f"; "format" ] ~doc ~docv:"<format>")

let verbose =
  let doc = "Say how objects are stored." in
  Arg.(value & flag & info [ "v"; "verbose" ] ~doc)

let root =
  let doc = "Root directory of the store." in
  Arg.(required & opt (some existing_directory) None & info [ "r"; "root" ] ~doc ~docv:"<dir>")

let output =
  let doc = "Output pack file." in
  Arg.(value & pos ~rev:true 0 fpath `Stdout & info [] ~doc ~docv:"<output>")

let like_git =
  let doc = "Objects are like Git objects." in
  Arg.(value & flag & info [ "like-git" ] ~doc)

let cmd ~digest =
  let doc = "Pack objects to a packed archive." in
  let exits = Term.default_exits in
  let man =
    [ `S Manpage.s_description
    ; `P "Pack objects to a packed archive." ] in
  Term.(const (pack ~digest) $ threads $ verbose $ root $ fformat $ output $ like_git),
  Term.info "pack" ~doc ~exits ~man
