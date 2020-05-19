open Carton

let blit_from_string src src_off dst dst_off len =
  Bigstringaf.blit_from_string src ~src_off dst ~dst_off ~len
[@@inline]

exception Exists

module Make (Scheduler : SCHEDULER) (IO : IO with type 'a t = 'a Scheduler.s) (Uid : UID) = struct
  let ( >>= ) x f = IO.bind x f
  let return x = IO.return x

  let sched =
    let open Scheduler in
    { Carton.bind= (fun x f -> inj (prj x >>= fun x -> prj (f x)))
    ; Carton.return= (fun x -> inj (return x)) }

  let read stream =
    let ke = Ke.Rke.create ~capacity:0x1000 Bigarray.char in

    let rec go filled inputs =
      match Ke.Rke.N.peek ke with
      | [] ->
        ( stream () >>= function
        | Some (src, off, len) ->
          Ke.Rke.N.push ke
            ~blit:blit_from_string
            ~length:String.length
            ~off ~len src ;
          go filled inputs
        | None -> return filled )
      | src :: _ ->
        let src = Cstruct.of_bigarray src in
        let len = min (Cstruct.len inputs) (Cstruct.len src) in
        Cstruct.blit src 0 inputs 0 len ;
        Ke.Rke.N.shift_exn ke len ;
        if len < Cstruct.len inputs
        then go (filled + len) (Cstruct.shift inputs len)
        else return (filled + len) in
    go

  module Verify = Carton.Dec.Verify(Uid)(Scheduler)(IO)
  module Fp = Carton.Dec.Fp(Uid)

  let first_pass ~zl_buffer ~digest stream =
    let fl_buffer = Cstruct.create 0x1000 in
    let zl_window = De.make_window ~bits:15 in
  
    let allocate _ = zl_window in

    let read_cstruct = read stream in
    let read_bytes () buf ~off ~len =
      let rec go rest raw =
        if rest <= 0
        then ( Cstruct.blit_to_bytes fl_buffer 0 buf off len
             ; return (abs rest + len) )
        else read_cstruct 0 raw >>= fun filled ->
          go (rest - filled) (Cstruct.shift raw filled) in
      go len fl_buffer in
    let read_bytes () buf ~off ~len = Scheduler.inj (read_bytes () buf ~off ~len) in
  
    Fp.check_header sched read_bytes () |> Scheduler.prj >>= fun (max, _, len) ->
  
    let decoder = Fp.decoder ~o:zl_buffer ~allocate `Manual in
    let decoder = Fp.src decoder (Cstruct.to_bigarray fl_buffer) 0 len in
  
    let children = Hashtbl.create 0x100 in
    let where    = Hashtbl.create 0x100 in
    let weight   = Hashtbl.create 0x100 in
    let matrix   = Array.make max Verify.unresolved_node in
  
    let rec go decoder = match Fp.decode decoder with
      | `Await decoder ->
        read_cstruct 0 fl_buffer >>= fun len ->
        go (Fp.src decoder (Cstruct.to_bigarray fl_buffer) 0 len)
      | `Peek decoder ->
        (* XXX(dinosaure): [Fp] does the compression. *)
        let keep = Fp.src_rem decoder in
        read_cstruct 0 (Cstruct.shift fl_buffer keep) >>= fun len ->
        go (Fp.src decoder (Cstruct.to_bigarray fl_buffer) 0 (keep + len))
      | `Entry ({ Fp.kind= Base _
                ; offset; size; _ }, decoder) ->
        let n = Fp.count decoder - 1 in
        Hashtbl.replace weight offset size ;
        Hashtbl.add where offset n ;
        matrix.(n) <- Verify.unresolved_base ~cursor:offset ;
        go decoder
      | `Entry ({ Fp.kind= Ofs { sub= s; source; target; }
                ; offset; _ }, decoder) ->
        let n = Fp.count decoder - 1 in
        Hashtbl.replace weight Int64.(sub offset (Int64.of_int s)) source ;
        Hashtbl.replace weight offset target ;
        Hashtbl.add where offset n ;
  
        ( try let vs = Hashtbl.find children (`Ofs Int64.(sub offset (of_int s))) in
            Hashtbl.replace children (`Ofs Int64.(sub offset (of_int s))) (offset :: vs)
          with Not_found ->
            Hashtbl.add children (`Ofs Int64.(sub offset (of_int s))) [ offset ] ) ;
        go decoder
      | `Entry ({ Fp.kind= Ref { ptr; target; _ }
                ; offset; _ }, decoder) ->
        let n = Fp.count decoder - 1 in
        Hashtbl.replace weight offset target ;
        Hashtbl.add where offset n ;
  
        ( try let vs = Hashtbl.find children (`Ref ptr) in
            Hashtbl.replace children (`Ref ptr) (offset :: vs)
          with Not_found ->
            Hashtbl.add children (`Ref ptr) [ offset ] ) ;
        go decoder
      | `End _ -> return (Ok ())
      | `Malformed err -> return (Error (`Msg err)) in
    go decoder >>= function
    | Error _ as err -> return err
    | Ok () ->
      return (Ok ({ Carton.Dec.where= (fun ~cursor -> Hashtbl.find where cursor)
                  ; children= (fun ~cursor ~uid ->
                        match Hashtbl.find_opt children (`Ofs cursor),
                              Hashtbl.find_opt children (`Ref uid) with
                        | Some a, Some b -> List.sort_uniq compare (a @ b)
                        | Some x, None | None, Some x -> x
                        | None, None -> [])
                  ; digest= digest
                  ; weight= (fun ~cursor -> Hashtbl.find weight cursor) },
                  matrix, where, children))

  type ('path, 'fd, 'error) fs =
    { create : 'path -> ('fd, 'error) result IO.t
    ; append : 'fd -> string -> unit IO.t
    ; map : 'fd -> pos:int64 -> int -> Bigstringaf.t IO.t
    ; close : 'fd -> unit IO.t }

  let ( >>? ) x f = x >>= function
    | Ok x -> f x
    | Error _ as err -> return err

  module Set = Set.Make(Uid)

  let zip a b =
    if Array.length a <> Array.length b then invalid_arg "zip: lengths mismatch" ;
    Array.init (Array.length a) (fun i -> a.(i), b.(i))

  let share l0 l1 =
    try List.iter (fun v -> if List.exists (( = ) v) l1 then raise Exists) l0 ; false
    with Exists -> true

  let verify ?(threads= 4) ~digest path { create; append; map; close; } stream =
    let zl_buffer = De.bigstring_create De.io_buffer_size in
    let allocate bits = De.make_window ~bits in
    let weight = ref 0L in
    create path >>? fun fd ->
    let stream () =
      stream () >>= function
      | Some (buf, off, len) as res ->
        append fd (String.sub buf off len) >>= fun () ->
        weight := Int64.add !weight (Int64.of_int len) ;
        return res
      | none -> return none in
    first_pass ~zl_buffer ~digest stream >>? fun (oracle, matrix, where, children) ->
    let weight = !weight in
    let pack = Carton.Dec.make fd ~allocate ~z:zl_buffer ~uid_ln:Uid.length ~uid_rw:Uid.of_raw_string
        (fun _ -> assert false) in
    let map fd ~pos len =
      let len = min len Int64.(to_int (sub weight pos)) in
      Scheduler.inj (map fd ~pos len) in
    Verify.verify ~threads pack ~map ~oracle ~matrix >>= fun () ->
    let offsets = Hashtbl.fold (fun k _ a -> k :: a) where []
                |> List.sort Int64.compare
                |> Array.of_list in
    let unresolveds = Array.fold_left (fun unresolveds (offset, status) ->
        if Verify.is_resolved status then unresolveds
        else offset :: unresolveds) [] (zip offsets matrix) in
    let requireds =
      Hashtbl.fold (fun k vs a -> match k with
          | `Ofs _ -> a
          | `Ref uid ->
            if share unresolveds vs
            then Set.add uid a else a)
      children Set.empty in
    close fd >>= fun () ->
    return (Ok (Hashtbl.length where, Set.elements requireds, weight))

  let find _ = assert false
  let vuid = { Carton.Enc.uid_ln= Uid.length
             ; Carton.Enc.uid_rw= Uid.to_raw_string }

  type light_load = Uid.t -> (Carton.kind * int) IO.t
  type heavy_load = Uid.t -> Carton.Dec.v IO.t
  type transmit = brk:int64 -> (bytes * int * int) IO.t

  let canonicalize ~light_load ~heavy_load ~transmit path { create; append; close; _ } n uids weight =
    let b =
      { Carton.Enc.o= Bigstringaf.create De.io_buffer_size
      ; Carton.Enc.i= Bigstringaf.create De.io_buffer_size
      ; Carton.Enc.q= De.Queue.create 0x10000
      ; Carton.Enc.w= De.make_window ~bits:15 } in
    let ctx = ref Uid.empty in
    let cursor = ref 0L in
    let heavy_load uid = Scheduler.inj (heavy_load uid) in
    create path >>= function
    | Error _ as err -> return err
    | Ok fd ->
      let header = Bigstringaf.create 12 in
      Carton.Enc.header_of_pack ~length:(n + List.length uids) header 0 12 ;
      let hdr = Bigstringaf.to_string header in
      append fd hdr >>= fun () ->
      ctx := Uid.feed !ctx header ;
      cursor := Int64.add !cursor 12L ;
      let encode_base uid =
        light_load uid >>= fun (kind, length) ->
        let entry = Carton.Enc.make_entry ~kind ~length uid in
        Carton.Enc.entry_to_target sched ~load:heavy_load entry |> Scheduler.prj >>= fun target ->
        Carton.Enc.encode_target sched ~b ~find ~load:heavy_load ~uid:vuid target ~cursor:(Int64.to_int !cursor)
        |> Scheduler.prj >>= fun (len, encoder) ->
        let rec go encoder = match Carton.Enc.N.encode ~o:b.o encoder with
          | `Flush (encoder, len) ->
            append fd (Bigstringaf.substring b.o ~off:0 ~len) >>= fun () ->
            ctx := Uid.feed !ctx ~off:0 ~len b.o ;
            cursor := Int64.add !cursor (Int64.of_int len) ;
            let encoder =
              Carton.Enc.N.dst encoder b.o 0 (Bigstringaf.length b.o) in
            go encoder
          | `End -> return () in
        append fd (Bigstringaf.substring b.o ~off:0 ~len) >>= fun () ->
        ctx := Uid.feed !ctx ~off:0 ~len b.o ;
        cursor := Int64.add !cursor (Int64.of_int len) ;
        let encoder = Carton.Enc.N.dst encoder b.o 0 (Bigstringaf.length b.o) in
        go encoder in
      let rec go = function
        | [] -> return ()
        | uid :: uids ->
          encode_base uid >>= fun () -> go uids in
      go uids >>= fun () ->
      let max = Int64.sub weight (Int64.of_int Uid.length) in
      let rec go brk =
        transmit ~brk >>= fun (payload, off, len) ->
        let llen = Int64.of_int len in
        if Int64.add brk llen > max
        then
          let llen = Int64.sub max brk in
          let len = Int64.to_int llen in
          let payload = Bytes.sub_string payload off len in
          append fd payload >>= fun () ->
          ctx := Uid.feed !ctx (Bigstringaf.of_string payload ~off:0 ~len) ;
          cursor := Int64.add !cursor llen ;
          let uid = Uid.get !ctx in
          append fd (Uid.to_raw_string uid) >>= fun () ->
          close fd >>= fun () ->
          return (Ok Int64.(add !cursor (of_int Uid.length)))
        else
          append fd (Bytes.sub_string payload off len) >>= fun () ->
          ctx := Uid.feed !ctx (Bigstringaf.of_string (Bytes.unsafe_to_string payload) ~off:off ~len) ;
          cursor := Int64.add !cursor llen ;
          go (Int64.add brk llen) in
      go 12L
end