open Prelude

let z = Dd.bigstring_create Dd.io_buffer_size
let allocate bits = Dd.make_window ~bits

let pp_kind ppf = function
  | `A -> Fmt.string ppf "A"
  | `B -> Fmt.string ppf "B"
  | `C -> Fmt.string ppf "C"
  | `D -> Fmt.string ppf "D"

let pp_path ppf path =
  let[@warning "-8"] x :: r = Clib.Dec.path_to_list path in
  Fmt.pf ppf "  %10d@," x ; List.iter (Fmt.pf ppf "Δ %10d@,") r ;

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
    let idx = Clib.Dec.Idx.make mp ~uid_ln:Uid.length ~uid_rw:Uid.to_raw_string ~uid_wr:Uid.of_raw_string in

    (fun () -> Unix.close fd),
    (fun uid -> match Clib.Dec.Idx.find idx uid with Some (_, off) -> off | None -> raise (Not_found uid))

let pp_stat ppf =
  Fmt.pf ppf "%a\n\
              \n\
              offset: %10d\n\
              weight: %10d\n\
              length: %10d\n\
              type:            %a\
              @[<hov>path: @[<v>%a@]@]\n%!"

let of_off ~(digest:Uid.t Clib.Dec.digest) off fpath =
  let open Rresult.R in
  let fd = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY ] 0o644 in
  let mx = let ic = Unix.in_channel_of_descr fd in in_channel_length ic in
  idx_of_pack fpath >>= fun (close_idx, idx) ->
  let pack = Clib.Dec.make { fd; mx; } ~z ~allocate ~uid_ln:Uid.length ~uid_rw:Uid.of_raw_string idx in

  let fiber () =
    let ( >>= ) = unix.Clib.bind in
    Clib.Dec.weight_of_offset unix ~map:unix_map pack ~weight:Clib.Dec.null ~cursor:off >>= fun weight ->
    Clib.Dec.path_of_offset unix ~map:unix_map pack ~cursor:off >>= fun path ->
    let raw = Clib.Dec.make_raw ~weight in
    Clib.Dec.of_offset_with_path unix ~map:unix_map pack ~path raw ~cursor:off >>= fun v ->
    let uid = digest ~kind:(Clib.Dec.kind v) ~off:0 ~len:(Clib.Dec.len v) (Clib.Dec.raw v) in
    pp_stat Fmt.stdout
      Uid.pp uid off (weight :> int) (Clib.Dec.len v) pp_kind (Clib.Dec.kind v) pp_path path ;
    Unix.close fd ; close_idx () ; unix.Clib.return () in

  match Us.prj (fiber ()) with
  | () -> Ok ()
  | exception No_idx ->
    Rresult.R.error_msgf "Packed archive must have associated index file."

let of_uid uid fpath =
  let stat = Unix.stat (Fpath.to_string fpath) in
  let fd0 = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY ] 0o644 in
  let mp = Mmap.V1.map_file fd0 ~pos:0L Bigarray.char Bigarray.c_layout false [| stat.Unix.st_size |] in
  let mp = Bigarray.array1_of_genarray mp in
  let idx = Clib.Dec.Idx.make mp ~uid_ln:Uid.length ~uid_rw:Uid.to_raw_string ~uid_wr:Uid.of_raw_string in

  let fpath = Fpath.set_ext "pack" fpath in
  let fd1 = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY ] 0o644 in
  let mx1 = let ic = Unix.in_channel_of_descr fd1 in in_channel_length ic in
  let pck = Clib.Dec.make { fd= fd1; mx= mx1; } ~z ~allocate ~uid_ln:Uid.length ~uid_rw:Uid.of_raw_string
      (fun uid -> match Clib.Dec.Idx.find idx uid with
         | Some (_, offset) -> offset
         | None -> raise (Not_found uid)) in

  let fiber () =
    let ( >>= ) = unix.Clib.bind in
    Clib.Dec.weight_of_uid unix ~map:unix_map pck ~weight:Clib.Dec.null uid >>= fun weight ->
    Clib.Dec.path_of_uid unix ~map:unix_map pck uid >>= fun path ->
    let raw = Clib.Dec.make_raw ~weight in
    Clib.Dec.of_uid unix ~map:unix_map pck raw uid >>= fun v ->
    let[@warning "-8"] off :: _ = Clib.Dec.path_to_list path in
    pp_stat Fmt.stdout
      Uid.pp uid off (weight :> int) (Clib.Dec.len v) pp_kind (Clib.Dec.kind v) pp_path path ;
    Unix.close fd0 ; Unix.close fd1 ; unix.Clib.return () in

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
  let parser x = match int_of_string x with
    | v -> Ok (`Ofs v)
    | exception (Failure _) -> match Uid.of_hex x with
      | v -> Ok (`Ref v)
      | exception (Invalid_argument _) -> Rresult.R.error_msgf "Invalid value %S" x in
  let pp ppf = function
    | `Ref v -> Uid.pp ppf v
    | `Ofs v -> Fmt.int ppf v in
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
