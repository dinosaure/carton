open Prelude

module Fpass = Cd.Fpass(Uid)
module Verify = Cd.Verify(Uid)(Us)(IO)

let z = Dd.bigstring_create Dd.io_buffer_size
let allocate bits = Dd.make_window ~bits

let zip a b =
  if Array.length a <> Array.length b then Fmt.invalid_arg "Array.zip" ;
  Array.init (Array.length a) (fun i -> a.(i), b.(i))

let pp_kind ppf = function
  | `A -> Fmt.string ppf "a"
  | `B -> Fmt.string ppf "b"
  | `C -> Fmt.string ppf "c"
  | `D -> Fmt.string ppf "d"

let pp_prev ppf (depth, source) = match source with
  | None -> Fmt.nop ppf ()
  | Some uid -> Fmt.pf ppf " %d %a" depth Uid.pp uid

let first_pass ~digest fpath =
  let ic = open_in (Fpath.to_string fpath) in
  let zw = Dd.make_window ~bits:15 in
  let allocate _ = zw in

  let (_, max) = Us.prj (Fpass.check_header unix unix_read ic) in
  seek_in ic 0 ;

  let decoder = Fpass.decoder ~o:z ~allocate (`Channel ic) in

  let children = Hashtbl.create 0x100 in
  let where = Hashtbl.create max in
  let weight = Hashtbl.create max in
  let length = Hashtbl.create max in
  let carbon = Hashtbl.create max in
  let matrix = Array.make max Verify.unresolved_node in

  let rec go decoder = match Fpass.decode decoder with
    | `Await _ | `Peek _ -> assert false
    | `Entry ({ Fpass.kind= Base _
              ; offset; size; consumed; }, decoder) ->
      let n = Fpass.count decoder - 1 in
      Hashtbl.add weight offset size ;
      Hashtbl.add length offset size ;
      Hashtbl.add carbon offset consumed ;
      Hashtbl.add where offset n ;
      matrix.(n) <- Verify.unresolved_base ~cursor:offset ;
      go decoder
    | `Entry ({ Fpass.kind= Ofs { sub; source; target; }
              ; offset; size; consumed; }, decoder) ->
      let n = Fpass.count decoder - 1 in
      Hashtbl.add weight (offset - sub) source ;
      Hashtbl.add weight offset target ;
      Hashtbl.add length offset size ;
      Hashtbl.add carbon offset consumed ;
      Hashtbl.add where offset n ;

      ( try let v = Hashtbl.find children (`Ofs (offset - sub)) in
          Hashtbl.add children (`Ofs (offset - sub)) (offset :: v)
        with Not_found ->
          Hashtbl.add children (`Ofs (offset - sub)) [ offset ] ) ;
      go decoder
    | `Entry _ -> assert false (* OBJ_REF *)
    | `End -> close_in ic ; Ok ()
    | `Malformed _ as err -> Error err in
  match go decoder with
  | Error _ as err -> err
  | Ok () ->
    Ok ({ Cd.where= (fun ~cursor -> Hashtbl.find where cursor)
        ; children= (fun ~cursor ~uid ->
              match Hashtbl.find_opt children (`Ofs cursor),
                    Hashtbl.find_opt children (`Ref uid) with
              | Some a, Some b -> List.sort_uniq (compare : int -> int -> int) (a @ b)
              | Some x, None | None, Some x -> x
              | None, None -> [])
        ; digest= digest
        ; weight= (fun ~cursor -> Hashtbl.find weight cursor) },
        matrix, where, length, carbon)

exception Invalid_pack

let print matrix (length : (int, Cd.weight) Hashtbl.t) carbon =
  Array.iter
    (fun (offset, s) ->
       let uid = Verify.uid_of_status s in
       let kind = Verify.kind_of_status s in
       let size = Hashtbl.find length offset in
       let consumed = Hashtbl.find carbon offset in
       let prev = Verify.depth_of_status s, Verify.source_of_status s in

       Fmt.pr "%a %a %d %d %d%a\n%!"
         Uid.pp uid pp_kind kind (size :> int) consumed offset
         pp_prev prev)
    matrix

let verify ~digest verbose idx fpath =
  let open Rresult.R in
  first_pass ~digest fpath >>= fun (oracle, matrix, where, length, carbon) ->
  let fd = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY ] 0o644 in
  let mx =
    let ic = Unix.in_channel_of_descr fd in
    in_channel_length ic in
  let index _ = raise Not_found in
  let t = Cd.make { fd; mx; } ~z ~allocate ~uid_ln:Uid.length ~uid_rw:Uid.of_raw_string index in

  Verify.verify ~map:unix_map ~oracle t ~matrix ;

  let offsets =
    Hashtbl.fold (fun k _ a -> k :: a) where []
    |> List.sort (Stdlib.compare : int -> int -> int)
    |> Array.of_list in
  let matrix = zip offsets matrix in

  try
    Array.iter
      (fun (offset, s) ->
         let uid = Verify.uid_of_status s in

         match Cd.Idx.find idx uid with
         | Some (_, offset') ->
           if offset != offset' then raise Invalid_pack ;
         | None -> raise Invalid_pack)
      matrix ; if verbose then print matrix length carbon ; Unix.close fd ; Ok ()
  with Invalid_pack -> Error `Invalid_pack

let verify ~digest verbose fpath =
  let stat = Unix.stat (Fpath.to_string fpath) in
  let fd = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY ] 0o644 in
  let mp = Mmap.V1.map_file fd ~pos:0L Bigarray.char Bigarray.c_layout false [| stat.Unix.st_size |] in
  let mp = Bigarray.array1_of_genarray mp in
  let idx = Cd.Idx.make mp ~uid_ln:Uid.length ~uid_rw:Uid.to_raw_string ~uid_wr:Uid.of_raw_string in

  let fpath = Fpath.set_ext "pack" fpath in

  let res = verify ~digest verbose idx fpath in Unix.close fd ; res

open Cmdliner

let existing_fpath ~ext =
  let parser x = match Fpath.of_string x with
    | Ok v when Sys.file_exists x ->
      if Fpath.has_ext ext v then Ok v else Rresult.R.error_msgf "%a has a bad extension" Fpath.pp v
    | Ok v -> Rresult.R.error_msgf "%a does not exist" Fpath.pp v
    | Error _ as err -> err in
  let pp = Fpath.pp in
  Arg.conv (parser, pp)

let uid =
  let parser x = match Uid.of_hex x with
    | v -> Ok v
    | exception (Invalid_argument err) -> Error (`Msg err) in
  let pp = Uid.pp in
  Arg.conv (parser, pp)

let idx =
  let doc = "The idx file to verify." in
  Arg.(required & pos 0 ~rev:true (some (existing_fpath ~ext:"idx")) None & info [] ~docv:"<pack>.idx" ~doc)

let verbose =
  let doc = "After verifying the pack, show list of objects contained in the pack." in
  Arg.(value & flag & info [ "v"; "verbose" ] ~doc)

let cmd ~digest =
  let doc = "Validate packed archive files" in
  let exits = Term.default_exits in
  let man =
    [ `S Manpage.s_description
    ; `P "Reads given idx file for packed archive created with carton \
          command and verifies idx file and the corresponding pack file." ] in
  Term.(const (verify ~digest) $ verbose $ idx),
  Term.info "verify" ~doc ~exits ~man
