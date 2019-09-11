module Option = struct
  type 'a t = 'a option

  let iter f = function Some x -> f x | None -> ()
end

open Prelude

let z = Dd.bigstring_create Dd.io_buffer_size
let allocate bits = Dd.make_window ~bits

let x = Array.make 256 `None

let notzen =
  for i = 0 to 31 do x.(i) <- `Style (`Fg, `bit24 (0xaf, 0xd7, 0xff)) done ;
  for i = 48 to 57 do x.(i) <- `Style (`Fg, `bit24 (0xaf, 0xdf, 0x77)) done ;
  for i = 65 to 90 do x.(i) <- `Style (`Fg, `bit24 (0xff, 0xaf, 0x5f)) done ;
  for i = 97 to 122 do x.(i) <- `Style (`Fg, `bit24 (0xff, 0xaf, 0xd7)) done ;
  Hxd.O.colorscheme_of_array x

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

let of_off ~hex ~hxd off fpath =
  let open Rresult.R in
  let fd = Unix.openfile (Fpath.to_string fpath) Unix.[ O_RDONLY ] 0o644 in
  let mx = let st = Unix.LargeFile.fstat fd in st.Unix.LargeFile.st_size in
  idx_of_pack fpath >>= fun (close_idx, idx) ->
  let pack = Carton.Dec.make { fd; mx; } ~z ~allocate ~uid_ln:Uid.length ~uid_rw:Uid.of_raw_string idx in

  let fiber () =
    let ( >>= ) = unix.Carton.bind in
    Carton.Dec.weight_of_offset unix ~map:unix_map pack ~weight:Carton.Dec.null ~cursor:off >>= fun weight ->
    let raw = Carton.Dec.make_raw ~weight in
    Carton.Dec.of_offset unix ~map:unix_map pack raw ~cursor:off >>= fun v ->
    close_idx () ; unix.Carton.return v in
  match Us.prj (fiber ()) with
  | v ->
    let raw = Carton.Dec.raw v in
    let len = Carton.Dec.len v in
    if hex then Fmt.pr "@[<hov>%a@]\n%!" (Hxd_string.pp hxd) (Bigstringaf.substring raw ~off:0 ~len)
    else Fmt.pr "%s%!" (Bigstringaf.substring raw ~off:0 ~len) ; Ok ()
  | exception No_idx ->
    Rresult.R.error_msgf "Packed archive must have associated index file."
  | exception (Not_found uid) ->
    Rresult.R.error_msgf "Packed archive is not canonic (external object %a is needed)" Uid.pp uid

let of_uid ~hex ~hxd uid fpath =
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
    let raw = Carton.Dec.make_raw ~weight in
    Carton.Dec.of_uid unix ~map:unix_map pck raw uid >>= fun v ->
    Unix.close fd0 ; Unix.close fd1 ; unix.Carton.return v in

  match Us.prj (fiber ()) with
  | v ->
    let raw = Carton.Dec.raw v in
    let len = Carton.Dec.len v in
    if hex then Fmt.pr "@[<hov>%a@]\n%!" (Hxd_string.pp hxd) (Bigstringaf.substring raw ~off:0 ~len)
    else Fmt.pr "%s\n%!" (Bigstringaf.substring raw ~off:0 ~len) ; Ok ()
  | exception (Not_found uid) ->
    Rresult.R.error_msgf "Packed archive is not canonic (external object %a is needed)" Uid.pp uid

let get ~digest:_ fpath hex hxd value =
  match value with
  | `Ofs off ->
    of_off ~hex ~hxd off fpath
  | `Ref uid ->
    let open Rresult.R in
    let idx = Fpath.set_ext "idx" fpath in
    Bos.OS.File.exists idx >>= function
    | true -> of_uid ~hex ~hxd uid idx
    | false -> Rresult.R.error_msgf "Carton needs %a to extract %a" Fpath.pp idx Uid.pp uid

let hxd cols groupsize long uppercase =
  Hxd.O.xxd ?cols ?groupsize ?long ~uppercase notzen

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

let hex =
  let doc = "Print object like $(b,hexdump)." in
  Arg.(value & flag & info [ "X"; "hex" ] ~doc)

let cols =
  let pp = Fmt.int in
  let parser x = match int_of_string x with
    | n ->
      if n < 1 || n > 256
      then Rresult.R.error_msgf "Invalid <cols> value (must <= 256 && > 0): %d" n
      else Rresult.R.ok n
    | exception _ ->
      Rresult.R.error_msgf "Invalid <cols> %S" x in
  Arg.conv ~docv:"<cols>" (parser, pp)

let unsigned =
  let pp = Fmt.int in
  let parser x = match int_of_string x with
    | n ->
      if n < 0
      then Rresult.R.error_msgf "Invalid <n> value (must be positive)"
      else Rresult.R.ok n
    | exception _ ->
      Rresult.R.error_msgf "Invalid <n> %S" x in
  Arg.conv ~docv:"<n>" (parser, pp)

let cols =
  let doc = "Format <cols> octets per line. Default 16. Maximum 256." in
  Arg.(value & opt cols 16 & info [ "c"; "cols" ] ~doc)

let groupsize =
  let doc = "Separate the output of every <bytes> bytes (two hex characters) by whitespace. \
             Specify -g 0 to suppress groupgin. <bytes> defaults to 2." in
  Arg.(value & opt unsigned 2 & info [ "g"; "groupsize" ] ~doc ~docv:"<bytes>")

let long =
  let doc = "Stop after writing <len> octets." in
  Arg.(value & opt (some unsigned) None & info [ "l"; "len" ] ~doc ~docv:"<len>")

let uppercase =
  let doc = "use upper case hex letters. Default is lower case." in
  Arg.(value & flag & info [ "u" ] ~doc)

let style_renderer ?env () =
  let enum = [ "auto", None
             ; "always", Some `Ansi
             ; "never", Some `None ] in
  let color = Arg.enum enum in
  let enum_alts = Arg.doc_alts_enum enum in
  let doc = Fmt.strf "Colorize the output. $(docv) must be %s." enum_alts in
  Arg.(value & opt color None & info [ "color" ] ?env ~doc ~docv:"<when>")

let do_hxd style_renderer cols groupsize long uppercase =
  Option.iter (Hxd.Fmt.set_style_renderer Fmt.stdout) style_renderer ;
  Hxd.O.xxd ~cols ~groupsize ?long ~uppercase notzen

let setup_hxd =
  let env = Arg.env_var "CARTON_COLOR" in
  Term.(const do_hxd $ style_renderer ~env () $ cols $ groupsize $ long $ uppercase)

let cmd ~digest =
  let doc = "Extract object from a packed archive" in
  let exits = Term.default_exits in
  let man =
    [ `S Manpage.s_description
    ; `P "Extracts object associated to an $(i,uid) or an $(i,offset) from packed archive created with $(b,carton) \
          command." ] in
  Term.(const (get ~digest) $ pack $ hex $ setup_hxd $ value),
  Term.info "get" ~doc ~exits ~man
