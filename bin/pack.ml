module Us = Carton.Make(struct type 'a t = 'a end)

let unix =
  { Carton.bind= (fun x f -> f (Us.prj x))
  ; Carton.return= (fun x -> Us.inj x) }

type fd = { fd : Unix.file_descr; mx : int; }

let map : (fd, Us.t) Carton.W.map =
  fun fd ~pos len ->
  let payload =
    let len = min (fd.mx - pos) len in
    Mmap.V1.map_file fd.fd ~pos:(Int64.of_int pos)
      Bigarray.char Bigarray.c_layout false [| len |] in
  Us.inj (Bigarray.array1_of_genarray payload)

let z = Dd.bigstring_create Dd.io_buffer_size
let w = Dd.make_window ~bits:15
let allocate _ = w

let colors = Array.make 256 `None
let notzen =
  for i = 0 to 31   do colors.(i) <- `Style (`Fg, `bit24 (0xaf, 0xd7, 0xff)) done ;
  for i = 48 to 57  do colors.(i) <- `Style (`Fg, `bit24 (0xaf, 0xdf, 0x77)) done ;
  for i = 65 to 90  do colors.(i) <- `Style (`Fg, `bit24 (0xff, 0xaf, 0x5f)) done ;
  for i = 97 to 122 do colors.(i) <- `Style (`Fg, `bit24 (0xff, 0xaf, 0xd7)) done ;
  Hxd.O.colorscheme_of_array colors

let cfg = Hxd.O.xxd ~uppercase:true notzen

let raw_of_v { Carton.raw; Carton.len; _ } =
  if raw.Carton.flip
  then Bigstringaf.sub raw.Carton.raw0 ~off:0 ~len
  else Bigstringaf.sub raw.Carton.raw1 ~off:0 ~len

let hash_of_v ({ Carton.kind; Carton.len; _ } as v) =
  let ctx = Digestif.SHA1.empty in

  let ctx = match kind with
    | `A -> Digestif.SHA1.feed_string ctx (Fmt.strf "commit %d\000" len)
    | `B -> Digestif.SHA1.feed_string ctx (Fmt.strf "tree %d\000" len)
    | `C -> Digestif.SHA1.feed_string ctx (Fmt.strf "blob %d\000" len)
    | `D -> Digestif.SHA1.feed_string ctx (Fmt.strf "tag %d\000" len) in
  let ctx = Digestif.SHA1.feed_bigstring ctx (raw_of_v v) in
  Digestif.SHA1.get ctx

let () = Hxd.Fmt.set_style_renderer Fmt.stdout `Ansi

let () =
  if not (Sys.file_exists Sys.argv.(1))
  then Fmt.invalid_arg "Invalid file: %s" Sys.argv.(1) ;

  let fd = Unix.openfile Sys.argv.(1) Unix.[ O_RDONLY ] 0o644 in
  let mx =
    let ic = Unix.in_channel_of_descr fd in
    in_channel_length ic in
  let t = Carton.make { fd; mx; } ~z ~allocate (fun _ -> assert false) in
  let offset = int_of_string Sys.argv.(2) in

  let fiber =
    let ( >>= ) = unix.bind in
    Carton.weight_of_offset
      unix ~map t offset >>= fun weight ->
    let raw = Carton.make_raw ~weight in
    Carton.of_offset unix ~map t raw offset in
  let v = Us.prj fiber in
  let hash = hash_of_v v in
  let raw = raw_of_v v in

  Fmt.pr "@[<hov>%a@]\n\n%!" (Hxd_string.pp cfg) (Bigstringaf.to_string raw) ;
  Fmt.pr "> %a.\n%!" Digestif.SHA1.pp hash
