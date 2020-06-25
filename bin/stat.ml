open Prelude

let z = De.bigstring_create De.io_buffer_size
let allocate bits = De.make_window ~bits

let pp_kind ppf = function
  | `A -> Fmt.string ppf "A"
  | `B -> Fmt.string ppf "B"
  | `C -> Fmt.string ppf "C"
  | `D -> Fmt.string ppf "D"

let pp_path ppf path =
  let[@warning "-8"] x :: r = Carton.Dec.path_to_list path in
  Fmt.pf ppf "  %10Ld@," x ; List.iter (Fmt.pf ppf "Δ %10Ld@,") r ;

exception No_idx
exception Not_found of Uid.t

let idx_of_pack fpath =
  let open Rresult.R in
  let fpath = Fpath.set_ext "idx" fpath in
  Bos.OS.File.exists fpath >>| function
  | false ->
    (fun () -> ()), (fun _ -> raise No_idx)
  | true ->
    let stat = Unix.stat (Fpath.to_string fpath) in
    let fd = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY ] 0o644 in
    let mp = Mmap.V1.map_file fd ~pos:0L Bigarray.char Bigarray.c_layout false [| stat.Unix.st_size |] in
    let mp = Bigarray.array1_of_genarray mp in
    let idx = Carton.Dec.Idx.make mp ~uid_ln:Uid.length ~uid_rw:Uid.to_raw_string ~uid_wr:Uid.of_raw_string in

    (fun () -> Unix.close fd),
    (fun uid -> match Carton.Dec.Idx.find idx uid with Some (_, off) -> off | None -> raise (Not_found uid))

let pp_stat ppf =
  Fmt.pf ppf "%a\n\
              \n\
              offset: %10Ld\n\
              weight: %10d\n\
              length: %10d\n\
              type:            %a\
              @[<hov>path: @[<v>%a@]@]\n%!"

let of_off ~(digest:Uid.t Carton.Dec.digest) off fpath =
  let open Rresult.R in
  let fd = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY ] 0o644 in
  let mx = let st = Unix.LargeFile.fstat fd in st.Unix.LargeFile.st_size in
  idx_of_pack fpath >>= fun (close_idx, idx) ->
  let pack = Carton.Dec.make { fd; mx; } ~z ~allocate ~uid_ln:Uid.length ~uid_rw:Uid.of_raw_string idx in

  let fiber () =
    let ( >>= ) = unix.Carton.bind in
    Carton.Dec.weight_of_offset unix ~map:unix_map pack ~weight:Carton.Dec.null off >>= fun weight ->
    Carton.Dec.path_of_offset unix ~map:unix_map pack ~cursor:off >>= fun path ->
    let raw = Carton.Dec.make_raw ~weight in
    Carton.Dec.of_offset_with_path unix ~map:unix_map pack ~path raw ~cursor:off >>= fun v ->
    let uid = digest ~kind:(Carton.Dec.kind v) ~off:0 ~len:(Carton.Dec.len v) (Carton.Dec.raw v) in
    pp_stat Fmt.stdout
      Uid.pp uid off (weight :> int) (Carton.Dec.len v) pp_kind (Carton.Dec.kind v) pp_path path ;
    Unix.close fd ; close_idx () ; unix.Carton.return () in

  match Us.prj (fiber ()) with
  | () -> Ok ()
  | exception No_idx ->
    Rresult.R.error_msgf "Packed archive must have associated index file."

let of_uid uid fpath =
  let stat = Unix.stat (Fpath.to_string fpath) in
  let fd0 = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY ] 0o644 in
  let mp = Mmap.V1.map_file fd0 ~pos:0L Bigarray.char Bigarray.c_layout false [| stat.Unix.st_size |] in
  let mp = Bigarray.array1_of_genarray mp in
  let idx = Carton.Dec.Idx.make mp ~uid_ln:Uid.length ~uid_rw:Uid.to_raw_string ~uid_wr:Uid.of_raw_string in

  let fpath = Fpath.set_ext "pack" fpath in
  let fd1 = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY ] 0o644 in
  let mx1 = let st = Unix.LargeFile.fstat fd1 in st.Unix.LargeFile.st_size in
  let pck = Carton.Dec.make { fd= fd1; mx= mx1; } ~z ~allocate ~uid_ln:Uid.length ~uid_rw:Uid.of_raw_string
      (fun uid -> match Carton.Dec.Idx.find idx uid with
         | Some (_, offset) -> offset
         | None -> raise (Not_found uid)) in

  let fiber () =
    let ( >>= ) = unix.Carton.bind in
    Carton.Dec.weight_of_uid unix ~map:unix_map pck ~weight:Carton.Dec.null uid >>= fun weight ->
    Carton.Dec.path_of_uid unix ~map:unix_map pck uid >>= fun path ->
    let raw = Carton.Dec.make_raw ~weight in
    Carton.Dec.of_uid unix ~map:unix_map pck raw uid >>= fun v ->
    let[@warning "-8"] off :: _ = Carton.Dec.path_to_list path in
    pp_stat Fmt.stdout
      Uid.pp uid off (weight :> int) (Carton.Dec.len v) pp_kind (Carton.Dec.kind v) pp_path path ;
    Unix.close fd0 ; Unix.close fd1 ; unix.Carton.return () in

  match Us.prj (fiber ()) with
  | () -> Ok ()
  | exception (Not_found uid) ->
    Rresult.R.error_msgf "Packed archive is not canonic (external object %a is needed)" Uid.pp uid

let stat ~digest fpath value =
  match value with
  | `Ofs off -> of_off ~digest off fpath
  | `Ref uid ->
    let open Rresult.R in
    let idx = Fpath.set_ext "idx" fpath in
    Bos.OS.File.exists idx >>= function
    | true -> of_uid uid idx
    | false -> Rresult.R.error_msgf "Carton needs %a to extract %a" Fpath.pp idx Uid.pp uid

open Cmdliner

let existing_fpath ~ext =
  let parser x = match Fpath.of_string x with
    | Ok v when Sys.file_exists x ->
      if Fpath.has_ext ext v then Ok v else Rresult.R.error_msgf "%a has a bad extension" Fpath.pp v
    | Ok v -> Rresult.R.error_msgf "%a does not exist" Fpath.pp v
    | Error _ as err -> err in
  let pp = Fpath.pp in
  Arg.conv (parser, pp)

let uid_or_offset =
  let parser x = match Int64.of_string x with
    | v -> Ok (`Ofs v)
    | exception (Failure _) -> match Uid.of_hex x with
      | v -> Ok (`Ref v)
      | exception (Invalid_argument _) -> Rresult.R.error_msgf "Invalid value %S" x in
  let pp ppf = function
    | `Ref v -> Uid.pp ppf v
    | `Ofs v -> Fmt.int64 ppf v in
  Arg.conv (parser, pp)

let pack =
  let doc = "The packed archive." in
  Arg.(required & pos 1 ~rev:true (some (existing_fpath ~ext:"pack")) None & info [] ~docv:"<pack>.pack" ~doc)

let value =
  let doc = "Uid or offset of object." in
  Arg.(required & pos 0 ~rev:true (some uid_or_offset) None & info [] ~docv:"<value>" ~doc)

let cmd ~digest =
  let doc = "Extract object from a packed archive" in
  let exits = Term.default_exits in
  let man =
    [ `S Manpage.s_description
    ; `P "Shows informations about object associated to an $(i,uid) or an $(i,offset) \
          from packed archive created with $(b,carton)" ] in
  Term.(const (stat ~digest) $ pack $ value),
  Term.info "stat" ~doc ~exits ~man
