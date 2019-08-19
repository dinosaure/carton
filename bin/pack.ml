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

module IO : Carton.IO with type 'a t = 'a Lwt.t = struct
  type 'a t = 'a Lwt.t
  type 'a u = 'a Lwt.u

  let bind = Lwt.bind
  let return = Lwt.return

  let list_iteri = Lwt_list.iteri_s

  type mutex = Lwt_mutex.t

  let mutex = Lwt_mutex.create
  let mutex_lock = Lwt_mutex.lock
  let mutex_unlock = Lwt_mutex.unlock

  let wakeup = Lwt.wakeup
  let async f = Lwt.async (fun () -> Lwt_preemptive.detach f ())
  let task = Lwt.task
  let join = Lwt.join
end

module Verify = Carton.Verify(IO)
module Ls = Verify.Scheduler

let lwt =
  let open Lwt.Infix in
  { Carton.bind= (fun x f -> Ls.inj (Ls.prj x >>= fun x -> Ls.prj (f x)))
  ; Carton.return = (fun x -> Ls.inj (Lwt.return x)) }

let lwt_map : (fd, Ls.t) Carton.W.map =
  fun fd ~pos len ->
  let payload =
    let len = min (fd.mx - pos) len in
    Mmap.V1.map_file fd.fd ~pos:(Int64.of_int pos)
      Bigarray.char Bigarray.c_layout false [| len |] in
  Ls.inj (Lwt.return (Bigarray.array1_of_genarray payload))

let z = Dd.bigstring_create Dd.io_buffer_size
let allocate bits = Dd.make_window ~bits

let raw_of_v = Carton.raw

let hash_of_v v =
  let ctx = Digestif.SHA1.empty in
  let len = Carton.len v in

  let ctx = match Carton.kind v with
    | `A -> Digestif.SHA1.feed_string ctx (Fmt.strf "commit %d\000" len)
    | `B -> Digestif.SHA1.feed_string ctx (Fmt.strf "tree %d\000" len)
    | `C -> Digestif.SHA1.feed_string ctx (Fmt.strf "blob %d\000" len)
    | `D -> Digestif.SHA1.feed_string ctx (Fmt.strf "tag %d\000" len) in
  let ctx = Digestif.SHA1.feed_bigstring ctx ~off:0 ~len (raw_of_v v) in
  Digestif.SHA1.get ctx

let digest ~kind ?(off= 0) ?len buf =
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

let zip a b =
  if Array.length a <> Array.length b
  then Fmt.invalid_arg "Array.zip" ;
  Array.init (Array.length a) (fun i -> a.(i), b.(i))

let verify t =
  let ic = open_in Sys.argv.(1) in
  let allocate bits = Dd.make_window ~bits in
  let decoder = Carton.Fpass.decoder ~o:(Bigstringaf.create 0x1000) ~allocate (`Channel ic) in
  let children = Hashtbl.create 0x100 in
  let where = Hashtbl.create 0x100 in
  let weight = Hashtbl.create 0x100 in
  let length = Hashtbl.create 0x100 in
  let carbon = Hashtbl.create 0x100 in
  let matrix = ref [||] in

  let rec go decoder = match Carton.Fpass.decode decoder with
    | `Await _ | `Peek _ -> assert false
    | `Entry ({ Carton.Fpass.kind= Base _; offset; size; consumed; }, decoder) ->
      if Array.length !matrix <> Carton.Fpass.number decoder
      then ( Fmt.epr "%d object(s).\n%!" (Carton.Fpass.number decoder)
           ; matrix := Array.make (Carton.Fpass.number decoder) Verify.unresolved_node ) ;

      let n = Carton.Fpass.count decoder - 1 in
      Hashtbl.add weight offset size ;
      Hashtbl.add length offset size ;
      Hashtbl.add carbon offset consumed ;
      Hashtbl.add where offset n ;
      !matrix.(n) <- Verify.unresolved_base ~cursor:offset ;
      go decoder
    | `Entry ({ Carton.Fpass.kind= Ofs { sub; source; target; }; offset; size; consumed; }, decoder) ->
      if Array.length !matrix <> Carton.Fpass.number decoder
      then ( Fmt.epr "%d object(s).\n%!" (Carton.Fpass.number decoder)
           ; matrix := Array.make (Carton.Fpass.number decoder) Verify.unresolved_node ) ;

      let n = Carton.Fpass.count decoder - 1 in
      Hashtbl.add weight (offset - sub) source ;
      Hashtbl.add length offset size ;
      Hashtbl.add weight offset target ;
      Hashtbl.add carbon offset consumed ;
      Hashtbl.add where offset n ;
      ( try let v = Hashtbl.find children (`Ofs (offset - sub)) in Hashtbl.add children (`Ofs (offset - sub)) (offset :: v)
        with Not_found -> Hashtbl.add children (`Ofs (offset - sub)) [ offset ] ) ;
      go decoder
    | `Entry _ -> assert false (* REF *)
    | `End -> close_in ic
    | `Malformed err -> failwith err in

  go decoder ;

  let oracle =
    { Carton.where= (fun ~cursor -> Hashtbl.find where cursor)
    ; children= (fun ~cursor ~uid -> match Hashtbl.find_opt children (`Ofs cursor), Hashtbl.find_opt children (`Ref uid) with
          | Some a, Some b -> List.sort_uniq (compare : int -> int -> int) (a @ b)
          | Some l, None | None, Some l -> l
          | None, None -> [])
    ; digest= digest
    ; weight= (fun ~cursor -> Hashtbl.find weight cursor) } in

  let matrix = !matrix in
  let fiber = Verify.verify ~map:lwt_map ~oracle t ~matrix in

  Lwt_preemptive.init 2 4 ignore ;
  Fmt.epr "Start to verify!\n%!" ;
  Lwt_main.run fiber ;
  let offsets = Hashtbl.fold (fun k _ a -> k :: a) where [] |> List.sort (compare : int -> int -> int) |> Array.of_list in
  let matrix = zip offsets matrix in
  let pp_kind ppf = function
    | `A -> Fmt.string ppf "commit" | `B -> Fmt.string ppf "tree  " | `C -> Fmt.string ppf "blob  " | `D -> Fmt.string ppf "tag   " in
  let pp_prev ppf (depth, source) = match source with
    | None -> Fmt.nop ppf ()
    | Some uid -> Fmt.pf ppf " %d %a" depth Digestif.SHA1.pp uid in
  Array.iter (fun (offset, s) ->
      let uid = Verify.uid_of_status s in
      let kind = Verify.kind_of_status s in
      let size = Hashtbl.find length offset in
      let consumed = Hashtbl.find carbon offset in
      let prev = Verify.depth_of_status s, Verify.source_of_status s in
      Fmt.epr "%a %a %d %d %d%a\n%!"
        Digestif.SHA1.pp uid pp_kind kind size consumed offset pp_prev prev) matrix

let load t =
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

  Fmt.pr "> %a.\n%!" Digestif.SHA1.pp hash ;
  let len = Carton.len v in
  let payload = Bigstringaf.to_string (raw_of_v v) in
  Fmt.epr "%s%!" (String.sub payload 0 len)

let () =
  if not (Sys.file_exists Sys.argv.(1))
  then Fmt.invalid_arg "Invalid file: %s" Sys.argv.(1) ;

  let fd = Unix.openfile Sys.argv.(1) Unix.[ O_RDONLY ] 0o644 in
  let mx =
    let ic = Unix.in_channel_of_descr fd in
    in_channel_length ic in
  let t = Carton.make { fd; mx; } ~z ~allocate (fun _ -> assert false) in

  if Array.length Sys.argv = 2
  then verify t
  else load t

