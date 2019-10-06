open Prelude

let pp_kind ppf = function
  | `A -> Fmt.string ppf "a"
  | `B -> Fmt.string ppf "b"
  | `C -> Fmt.string ppf "c"
  | `D -> Fmt.string ppf "d"

let equal_kind a b = match a, b with
  | `A, `A | `B, `B | `C, `C | `D, `D -> true
  | _ -> false

let kind = Alcotest.testable pp_kind equal_kind
let optint = Alcotest.testable Optint.pp Optint.equal
let sha1 = Alcotest.testable Digestif.SHA1.pp Digestif.SHA1.equal
let s = Alcotest.testable (fun ppf x -> Fmt.pf ppf "%S" x) String.equal

let z = Bigstringaf.create Dd.io_buffer_size
let allocate bits = Dd.make_window ~bits
let o = Bigstringaf.create Dd.io_buffer_size

let test_empty_pack () =
  Alcotest.test_case "empty pack" `Quick @@ fun () ->
  let buf = Bigstringaf.create 12 in
  let ctx = Digestif.SHA1.empty in
  Carton.Enc.header_of_pack ~length:0 buf 0 12 ;
  let ctx = Digestif.SHA1.feed_bigstring ctx buf ~off:0 ~len:12 in
  let sha = Digestif.SHA1.get ctx in
  Alcotest.(check sha1) "hash" sha (Digestif.SHA1.of_hex "029d08823bd8a8eab510ad6ac75c823cfd3ed31e") ;
  let res = Bigstringaf.to_string buf ^ Digestif.SHA1.to_raw_string sha in
  Alcotest.(check s) "contents" res
    "PACK\000\000\000\002\000\000\000\000\
     \002\157\b\130;\216\168\234\181\016\173j\199\\\130<\253>\211\030"

let empty_pack =
  let v =
    "PACK\000\000\000\002\000\000\000\000\
     \002\157\b\130;\216\168\234\181\016\173j\199\\\130<\253>\211\030" in
  Bigstringaf.of_string v ~off:0 ~len:(String.length v)

module Fp = Carton.Dec.Fp(Uid)

type fake_file_descriptor =
  { mutable pos : int
  ; mutable lst : Bigstringaf.t list }

let fd_and_read_of_bigstring_list lst =
  let fd = { pos= 0; lst= lst } in
  let read fd buf ~off ~len =
    match fd.lst with
    | [] -> Us.inj 0
    | x :: r ->
      let len = min len (Bigstringaf.length x - fd.pos) in
      Bigstringaf.blit_to_bytes x ~src_off:fd.pos buf ~dst_off:off ~len ;
      fd.pos <- fd.pos + len ; if fd.pos = Bigstringaf.length x then fd.lst <- r ; Us.inj len in
  fd, read

let valid_empty_pack () =
  Alcotest.test_case "valid empty pack" `Quick @@ fun () ->
  let fd, read = fd_and_read_of_bigstring_list [ empty_pack ] in
  let max, buf = Us.prj (Fp.check_header unix read fd) in
  let tmp0 = Bytes.create Dd.io_buffer_size in
  let tmp1 = Bigstringaf.create Dd.io_buffer_size in

  let decoder = Fp.decoder ~o ~allocate `Manual in
  let decoder = Fp.src decoder (Bigstringaf.of_string buf ~off:0 ~len:(String.length buf)) 0 (String.length buf) in

  Alcotest.(check int) "number" max 0 ;

  let rec go decoder = match Fp.decode decoder with
    | `End uid ->
      Alcotest.(check sha1) "hash" uid
        (Digestif.SHA1.of_hex "029d08823bd8a8eab510ad6ac75c823cfd3ed31e")
    | `Entry _ -> Alcotest.fail "Unexpected entry"
    | `Malformed err -> Alcotest.fail err
    | `Await decoder ->
      let fiber = read fd tmp0 ~off:0 ~len:(Bytes.length tmp0) in
      let len = Us.prj fiber in
      Bigstringaf.blit_from_bytes tmp0 ~src_off:0 tmp1 ~dst_off:0 ~len ;
      let decoder = Fp.src decoder tmp1 0 len in
      go decoder
    | `Peek _ -> Alcotest.fail "Unexpected `Peek" in

  go decoder

module Verify = Carton.Dec.Verify(Uid)(Us)(IO)

let digest_like_git ~kind ?(off= 0) ?len buf =
  let len = match len with
    | Some len -> len
    | None -> Bigstringaf.length buf - off in
  let ctx = Digestif.SHA1.empty in

  let ctx = match kind with
    | `A -> Digestif.SHA1.feed_string ctx (Fmt.strf "commit %d\000" len)
    | `B -> Digestif.SHA1.feed_string ctx (Fmt.strf "tree %d\000" len)
    | `C -> Digestif.SHA1.feed_string ctx (Fmt.strf "blob %d\000" len)
    | `D -> Digestif.SHA1.feed_string ctx (Fmt.strf "tag %d\000" len) in
  let ctx = Digestif.SHA1.feed_bigstring ctx ~off ~len buf in
  Digestif.SHA1.get ctx

let verify_empty_pack () =
  Alcotest.test_case "verify empty pack" `Quick @@ fun () ->
  let t = Carton.Dec.make () ~z ~allocate ~uid_ln:Uid.length ~uid_rw:Uid.of_raw_string (fun _ -> Alcotest.fail "Invalid call to IDX") in
  let map () ~pos length =
    let len = min length (Int64.to_int pos - Bigstringaf.length empty_pack) in
    Us.inj (Bigstringaf.sub empty_pack ~off:(Int64.to_int pos) ~len) in
  let oracle =
    { Carton.Dec.digest= digest_like_git
    ; children= (fun ~cursor:_ ~uid:_ -> [])
    ; where= (fun ~cursor:_ -> Alcotest.fail "Invalid call to [where]")
    ; weight= (fun ~cursor:_ -> Alcotest.fail "Invalid call to [weight]") } in
  Verify.verify ~threads:1 ~map ~oracle t ~matrix:[||]

module Idx = Carton.Dec.Idx.N(Uid)

let empty_index =
  "\255tOc\000\000\000\002\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\
   \002\157\b\130;\216\168\234\181\016\173j\199\\\130<\253>\211\030\144\211\1474\226FGc\023\234F\158W\167\165\189\184Fth"

let index_of_empty_pack () =
  Alcotest.test_case "index of empty pack" `Quick @@ fun () ->
  let p = ref 0 and c = ref 0 in
  let encoder = Idx.encoder `Manual ~pack:(Uid.of_hex "029d08823bd8a8eab510ad6ac75c823cfd3ed31e") [||] in
  Idx.dst encoder o 0 (Bigstringaf.length o) ;

  let rec go () = match Idx.encode encoder `Await with
    | `Partial ->
      Alcotest.(check bool) "`Partial" (!c < 3) true ; incr c ;
      let pos = (Bigstringaf.length o - !p) - Idx.dst_rem encoder in
      Idx.dst encoder o pos (Bigstringaf.length o - pos) ; p := !p + pos ; go ()
    | `Ok ->
      let raw = Bigstringaf.substring o ~off:0 ~len:!p in
      Alcotest.(check s) "index" raw empty_index in
  go () ;
  let uid = Bigstringaf.substring o ~off:(!p - Uid.length) ~len:Uid.length in
  let uid = Uid.of_raw_string uid in
  Alcotest.(check sha1) "hash" uid (Uid.of_hex "90d39334e246476317ea469e57a7a5bdb8467468")

let check_empty_index () =
  Alcotest.test_case "check empty index" `Quick @@ fun () ->
  let map = Bigstringaf.of_string empty_index ~off:0 ~len:(String.length empty_index) in
  let idx = Carton.Dec.Idx.make map ~uid_ln:Uid.length ~uid_rw:Uid.to_raw_string ~uid_wr:Uid.of_raw_string in
  Alcotest.(check int) "number of entries" (Carton.Dec.Idx.max idx) 0

let index_of_one_entry () =
  Alcotest.test_case "index of one entry" `Quick @@ fun () ->
  let p = ref 0 and c = ref 0 in
  let encoder = Idx.encoder `Manual ~pack:(Uid.of_hex "") [| { Idx.crc= Checkseum.Crc32.default; offset= 0L; uid= Uid.null } |] in
  Idx.dst encoder o 0 (Bigstringaf.length o) ;

  let rec go () = match Idx.encode encoder `Await with
    | `Partial ->
      Alcotest.(check bool) "`Partial" (!c < 3) true ; incr c ;
      let pos = (Bigstringaf.length o - !p) - Idx.dst_rem encoder in
      Idx.dst encoder o pos (Bigstringaf.length o - pos) ; p := !p + pos ; go ()
    | `Ok -> Bigstringaf.sub o ~off:0 ~len:!p in
  let idx = Carton.Dec.Idx.make (go ()) ~uid_ln:Uid.length ~uid_rw:Uid.to_raw_string ~uid_wr:Uid.of_raw_string in
  Alcotest.(check int) "number of entries" (Carton.Dec.Idx.max idx) 1 ;
  Alcotest.(check (option (pair optint int64))) "entry" (Carton.Dec.Idx.find idx Uid.null) (Some (Checkseum.Crc32.default, 0L))

type fd_with_length = { fd : Unix.file_descr; mx : int64; }

let map { fd; mx; } ~pos len =
  let len = min Int64.(sub mx pos) (Int64.of_int len) in
  let mp = Unix.map_file fd ~pos Bigarray.char Bigarray.c_layout false [| Int64.to_int len |] in
  let mp = Bigarray.array1_of_genarray mp in
  Us.inj mp

let index_of_bomb_pack () =
  Alcotest.test_case "index of bomb pack" `Quick @@ fun () ->
  let o = Bigstringaf.create 0x1000 in
  let allocate bits = Dd.make_window ~bits in
  let decoder = Fp.decoder ~o ~allocate `Manual in
  let tmp0 = Bytes.create 0x1000 in
  let tmp1 = Bigstringaf.create 0x1000 in

  let ic = open_in "bomb.pack" in

  let max, buf = Fp.check_header unix (fun ic buf ~off ~len -> Us.inj (input ic buf off len)) ic |> Us.prj in
  let decoder = Fp.src decoder (Bigstringaf.of_string buf ~off:0 ~len:12) 0 12 in

  let weight = Hashtbl.create max in
  let children = Hashtbl.create max in
  let where = Hashtbl.create max in
  let matrix = Array.make max Verify.unresolved_node in

  let rec go decoder = match Fp.decode decoder with
    | `Await decoder ->
      Fmt.epr "`AWAIT\n%!" ;

      let len = input ic tmp0 0 0x1000 in
      Bigstringaf.blit_from_bytes tmp0 ~src_off:0 tmp1 ~dst_off:0 ~len ;
      let decoder = Fp.src decoder tmp1 0 len in
      go decoder
    | `Peek decoder ->
      let keep = Fp.src_rem decoder in
      let len = input ic tmp0 0 (0x1000 - keep) in
      Bigstringaf.blit_from_bytes tmp0 ~src_off:0 tmp1 ~dst_off:keep ~len ;
      let decoder = Fp.src decoder tmp1 0 (keep + len) in
      go decoder
    | `Entry ({ Fp.kind= Base _
              ; offset; size; _ }, decoder) ->
      let n = Fp.count decoder - 1 in
      Hashtbl.add weight offset size ;
      Hashtbl.add where offset n ;
      matrix.(n) <- Verify.unresolved_base ~cursor:offset ;
      go decoder
    | `Entry ({ Fp.kind= Ofs { sub= s; source; target; }
              ; offset; _ }, decoder) ->
      let n = Fp.count decoder - 1 in
      let base = Int64.(sub offset (of_int s)) in

      Hashtbl.add weight base source ;
      Hashtbl.add weight offset target ;
      Hashtbl.add where offset n ;

      ( try let v = Hashtbl.find children (`Ofs base) in
          Hashtbl.add children (`Ofs base) (offset :: v)
        with Not_found ->
          Hashtbl.add children (`Ofs base) [ offset ] ) ;
      go decoder
    | `Entry _ -> (* OBJ_REF *) Alcotest.fail "Unexpected OBJ_REF"
    | `Malformed err -> Alcotest.fail err
    | `End uid ->
      Alcotest.(check sha1) "hash" uid (Uid.of_hex "d1c2ce2fc6dfaaa18d0ea1b564334d738b0e2339") in

  go decoder ; close_in ic ;
  let fd = Unix.openfile "bomb.pack" Unix.[ O_RDONLY ] 0o644 in
  let mx = let st = Unix.LargeFile.fstat fd in st.Unix.LargeFile.st_size in

  let oracle =
    { Carton.Dec.where= (fun ~cursor -> Hashtbl.find where cursor)
    ; children= (fun ~cursor ~uid ->
          match Hashtbl.find_opt children (`Ofs cursor),
                Hashtbl.find_opt children (`Ref uid) with
          | Some a, Some b -> List.sort_uniq compare (a @ b)
          | Some x, None | None, Some x -> x
          | None, None -> [])
    ; digest= digest_like_git
    ; weight= (fun ~cursor -> Hashtbl.find weight cursor) } in

  let z = Bigstringaf.create 0x1000 in
  let t = Carton.Dec.make { fd; mx; } ~z ~allocate ~uid_ln:Uid.length ~uid_rw:Uid.of_raw_string
      (fun _ -> Alcotest.fail "Invalid call to IDX") in
  Verify.verify ~threads:1 ~map ~oracle t ~matrix ; Unix.close fd

let first_entry_of_bomb_pack () =
  Alcotest.test_case "first entry of bomb pack" `Quick @@ fun () ->
  let fd = Unix.openfile "bomb.pack" Unix.[ O_RDONLY ] 0o644 in
  let mx = let st = Unix.LargeFile.fstat fd in st.Unix.LargeFile.st_size in
  let pack = Carton.Dec.make { fd; mx; } ~z ~allocate ~uid_ln:Uid.length ~uid_rw:Uid.of_raw_string (fun _ -> Alcotest.fail "Invalid call to IDX") in
  let fiber () =
    let ( >>= ) = unix.Carton.bind in

    Carton.Dec.weight_of_offset unix ~map pack ~weight:Carton.Dec.null ~cursor:12L >>= fun weight ->
    let raw = Carton.Dec.make_raw ~weight in
    Carton.Dec.of_offset unix ~map pack raw ~cursor:12L >>= fun v ->
    unix.Carton.return v in
  let v = Us.prj (fiber ()) in
  Alcotest.(check kind) "kind" (Carton.Dec.kind v) `A ;
  Alcotest.(check int) "length" (Carton.Dec.len v) 218 ;
  Alcotest.(check int) "depth" (Carton.Dec.depth v) 1

let () =
  Alcotest.run "carton"
    [ "encoder", [ test_empty_pack ()
                 ; index_of_empty_pack ()
                 ; index_of_one_entry () ]
    ; "decoder", [ valid_empty_pack ()
                 ; verify_empty_pack ()
                 ; check_empty_index ()
                 ; index_of_bomb_pack ()
                 ; first_entry_of_bomb_pack () ] ]
