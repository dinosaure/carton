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

let raw_of_v = Carton.raw

let hash_of_v v =
  let ctx = Digestif.SHA1.empty in
  let len = Carton.len v in

  let ctx = match Carton.kind v with
    | `A -> Digestif.SHA1.feed_string ctx (Fmt.strf "commit %d\000" len)
    | `B -> Digestif.SHA1.feed_string ctx (Fmt.strf "tree %d\000" len)
    | `C -> Digestif.SHA1.feed_string ctx (Fmt.strf "blob %d\000" len)
    | `D -> Digestif.SHA1.feed_string ctx (Fmt.strf "tag %d\000" len) in
  let ctx = Digestif.SHA1.feed_bigstring ctx (raw_of_v v) in
  Digestif.SHA1.get ctx

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
      unix ~map t ~weight:Carton.null ~cursor:offset >>= fun weight ->
    let raw = Carton.make_raw ~weight in
    Carton.of_offset unix ~map t raw ~cursor:offset in
  let v = Us.prj fiber in
  let hash = hash_of_v v in

  Fmt.pr "> %a.\n%!" Digestif.SHA1.pp hash ;

  let fiber = Carton.path_of_offset unix ~map t ~cursor:offset in
  let path = Us.prj fiber in

  Fmt.pr "> @[<hov>%a@]\n%!" Carton.pp_path path ;

  let fiber =
    let ( >>= ) = unix.bind in
    Carton.weight_of_offset
      unix ~map t ~weight:Carton.null ~cursor:offset >>= fun weight ->
    let raw = Carton.make_raw ~weight in
    Carton.of_offset_with_path unix ~map t ~path raw ~cursor:offset in
  let v = Us.prj fiber in
  let hash = hash_of_v v in

  Fmt.pr "> %a.\n%!" Digestif.SHA1.pp hash
