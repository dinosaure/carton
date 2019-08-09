module type FUNCTOR = sig type 'a t end

type ('a, 's) io

type 's scheduler =
  { bind : 'a 'b. ('a, 's) io -> ('a -> ('b, 's) io) -> ('b, 's) io
  ; return : 'a. 'a -> ('a, 's) io }

module type SCHEDULER = sig
  type 'a s
  type t

  external inj : 'a s -> ('a, t) io = "%identity"
  external prj : ('a, t) io -> 'a s = "%identity"
end

module Make (T : FUNCTOR) : SCHEDULER with type 'a s = 'a T.t = struct
  type 'a s = 'a T.t
  type t

  external inj : 'a -> 'b = "%identity"
  external prj : 'a -> 'b = "%identity"
end

type bigstring = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

let bigstring_length : bigstring -> int = Bigstringaf.length
let unsafe_get_uint8 : bigstring -> int -> int = fun v off -> Char.code (Bigstringaf.get v off)
let unsafe_get_char : bigstring -> int -> char = Bigstringaf.get

type checksum

module W = struct
  type 'fd t =
    { mutable cur : int
    ; w : slice option array
    ; m : int
    ; fd : 'fd }
  and slice = { offset : int
              ; length : int
              ; payload : bigstring }
  and ('fd, 's) map = 'fd -> pos:int -> int -> (bigstring, 's) io

  let make fd =
    { cur= 0
    ; w= Array.make 0xf None
    ; m= 0xf
    ; fd }

  (* XXX(dinosaure): memoization. *)

  let heavy_load
    : type fd s. s scheduler -> map:(fd, s) map -> fd t -> int -> (slice option, s) io
    = fun { bind; return; } ~map t w ->
      let ( >>= ) = bind in
      map t.fd
        ~pos:(w / 1024 * 1024)
        (1024 * 1024) >>= fun payload ->
      let slice = Some { offset= (w / 1024 * 1024)
                       ; length= bigstring_length payload
                       ; payload; } in
      t.w.(t.cur land 7) <- slice ;
      t.cur <- t.cur + 1 ; return slice

  let load
    : type fd s. s scheduler -> map:(fd, s) map -> fd t -> int -> (slice option, s) io
    = fun ({ return; _ } as s) ~map t w ->
    let exception Found in
    let slice = ref None in
    try
      Array.iter (function
          | Some ({ offset; length; _ } as s) ->
            if w >= offset && w < offset + length
            then ( slice := Some s ; raise_notrace Found )
          | None -> () )
        t.w ;
      heavy_load s ~map t w
    with Found -> return !slice
end

module Uid = struct
  type t = Digestif.SHA1.t

  let length = Digestif.SHA1.digest_size
  let of_raw_string : string -> t = Digestif.SHA1.of_raw_string
  let pp = Digestif.SHA1.pp
end

type 'fd t =
  { ws : 'fd W.t
  ; fd : Uid.t -> int
  ; cr : Uid.t -> checksum
  ; tmp : Zz.bigstring
  ; allocate : int -> Zz.window }

let make
  : type fd. fd -> z:Zz.bigstring -> allocate:(int -> Zz.window) -> (Uid.t -> int) -> fd t
  = fun fd ~z ~allocate where ->
    { ws= W.make fd
    ; fd= where
    ; cr= (fun _ -> assert false)
    ; tmp= z
    ; allocate }

type weight = int

let null = 0

let weight_of_delta
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> weight:weight -> cursor:int -> W.slice -> (weight, s) io
  = fun ({ return; bind; } as s) ~map t ~weight ~cursor slice ->
    let ( >>= ) = bind in
    let decoder = Zh.M.decoder ~o:t.tmp ~allocate:t.allocate `Manual in
    let rec go cursor decoder = match Zh.M.decode decoder with
      | `End _ -> assert false (* XXX(dinosaure): [`End] never appears before [`Header]. *)
      | `Malformed err -> failwith err
      | `Header (src_len, dst_len, _) -> return (max weight (max src_len dst_len))
      | `Await decoder ->
        W.load s ~map t.ws cursor >>= function
        | None ->
          let decoder = Zh.M.src decoder Dd.bigstring_empty 0 0 in
          (* XXX(dinosaure): End of stream, [Zh] should return [`Malformed] then. *)
          (go[@tailcall]) cursor decoder
        | Some slice ->
          let decoder = Zh.M.src decoder slice.W.payload 0 slice.W.length in
          (go[@tailcall]) (slice.W.offset + slice.W.length) decoder in
    let off = cursor - slice.W.offset in
    let len = slice.W.length - off in
    let decoder = Zh.M.src decoder slice.W.payload off len in
    go (slice.W.offset + slice.W.length) decoder

let header_of_ref_delta ({ bind; return; } as s) ~map t cursor slice =
  let ( >>= ) = bind in
  let slice = ref slice in
  let i_pos = ref (cursor - !slice.W.offset) in
  let i_rem = !slice.W.length - !i_pos in

  let fiber =
    if i_rem >= Uid.length
    then return (fun () -> incr i_pos)
    else
      W.load s ~map t.ws (!slice.W.offset + !slice.W.length) >>= function
      | None -> assert false
      | Some next_slice ->
        let consume () =
          incr i_pos ;
          if !i_pos == !slice.W.length
          then ( assert (!slice != next_slice)
               ; i_pos := 0
               ; slice := next_slice ) in
        return consume in
  fiber >>= fun consume ->

  let uid =
    if i_rem >= Uid.length
    then
      let uid = Bigstringaf.substring !slice.W.payload ~off:!i_pos ~len:Uid.length in
      let uid = Uid.of_raw_string uid in
      for _ = 0 to Uid.length - 1 do consume () done ; uid
    else
      let uid = Bytes.create Uid.length in
      for i = 0 to Uid.length - 1 do
        Bytes.unsafe_set uid i (unsafe_get_char !slice.W.payload !i_pos) ;
        consume ()
      done ; Uid.of_raw_string (Bytes.unsafe_to_string uid) in

  return (uid, !i_pos, !slice)

let header_of_ofs_detla ({ bind; return; } as s) ~map t cursor slice =
  let ( >>= ) = bind in
  let slice = ref slice in
  let i_pos = ref (cursor - !slice.W.offset) in
  let i_rem = !slice.W.length - !i_pos in

  let fiber =
    if i_rem >= Uid.length
    then return (fun () -> incr i_pos)
    else
      W.load s ~map t.ws (!slice.W.offset + !slice.W.length) >>= function
      | None -> assert false
      | Some next_slice ->
        let consume () =
          incr i_pos ;
          if !i_pos == !slice.W.length
          then ( assert (!slice != next_slice)
               ; i_pos := 0
               ; slice := next_slice ) in
        return consume in
  fiber >>= fun consume ->

  let c = ref (unsafe_get_uint8 !slice.W.payload !i_pos) in
  consume () ;
  let base_offset = ref (!c land 127) in

  while !c land 128 != 0
  do
    incr base_offset ;
    c := unsafe_get_uint8 !slice.W.payload !i_pos ;
    consume () ;
    base_offset := (!base_offset lsl 7) + (!c land 127) ;
  done ;

  return (!base_offset, !i_pos, !slice)

let header_of_entry ({ bind; return; } as s) ~map t cursor slice =
  let ( >>= ) = bind in
  let slice = ref slice in
  let i_pos = ref (cursor - !slice.W.offset) in
  let i_rem = !slice.W.length - !i_pos in

  let fiber =
    if i_rem >= Uid.length
    then return (fun () -> incr i_pos)
    else
      W.load s ~map t.ws (!slice.W.offset + !slice.W.length) >>= function
      | None -> assert false
      | Some next_slice ->
        let consume () =
          incr i_pos ;
          if !i_pos == !slice.W.length
          then ( assert (!slice != next_slice)
               ; i_pos := 0
               ; slice := next_slice ) in
        return consume in
  fiber >>= fun consume ->

  let c = ref (unsafe_get_uint8 !slice.W.payload !i_pos) in
  consume () ;
  let kind = (!c asr 4) land 7 in
  let size = ref (!c land 15) in
  let shft = ref 4 in

  while !c land 0x80 != 0
  do
    c := unsafe_get_uint8 !slice.W.payload !i_pos ;
    consume () ;
    size := !size + ((!c land 0x7f) lsl !shft) ;
    shft := !shft + 7 ;
  done ;

  return (kind, !size, !i_pos, !slice)

let rec weight_of_ref_delta
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> weight:weight -> cursor:int -> W.slice -> (weight, s) io
  = fun ({ bind; _ } as s) ~map t ~weight ~cursor slice ->
    let ( >>= ) = bind in

    header_of_ref_delta s ~map t cursor slice >>= fun (uid, pos, slice) ->
    weight_of_delta s ~map t ~weight ~cursor:(slice.W.offset + pos) slice >>= fun weight ->
    (weight_of_uid[@tailcall]) s ~map t ~weight uid

and weight_of_ofs_delta
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> weight:weight -> anchor:int -> cursor:int -> W.slice -> (weight, s) io
  = fun ({ bind; _ } as s) ~map t ~weight ~anchor ~cursor slice ->
    let ( >>= ) = bind in

    header_of_ofs_detla s ~map t cursor slice >>= fun (base_offset, pos, slice) ->
    weight_of_delta s ~map t ~weight ~cursor:(slice.W.offset + pos) slice >>= fun weight ->
    (weight_of_offset[@tailcall]) s ~map t ~weight ~cursor:(anchor - base_offset)

and weight_of_uid
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> weight:weight -> Uid.t -> (weight, s) io
  = fun s ~map t ~weight uid ->
    let cursor = t.fd uid in
    (weight_of_offset[@tailcall]) s ~map t ~weight ~cursor

and weight_of_offset
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> weight:weight -> cursor:int -> (weight, s) io
  = fun ({ bind; return; } as s) ~map t ~weight ~cursor ->
    let ( >>= ) = bind in

    W.load s ~map t.ws cursor >>= function
    | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_offset])" cursor
    | Some slice ->
      header_of_entry s ~map t cursor slice >>= fun (kind, size, pos, slice) ->

      match kind with
      | 0b000 | 0b101 -> failwith "bad type"
      | 0b001 | 0b010 | 0b011 | 0b100 -> return (max size weight)
      | 0b110 -> (weight_of_ofs_delta[@tailcall]) s ~map t ~weight:(max size weight) ~anchor:cursor ~cursor:(slice.W.offset + pos) slice
      | 0b111 -> (weight_of_ref_delta[@tailcall]) s ~map t ~weight:(max size weight) ~cursor:(slice.W.offset + pos) slice
      | _ -> assert false

type raw =
  { raw0 : bigstring
  ; raw1 : bigstring
  ; flip : bool }

type v =
  { kind : [ `A | `B | `C | `D ]
  ; raw  : raw
  ; len  : int }

let kind { kind; _ } = kind

let make_raw ~weight =
  let raw = Bigstringaf.create (weight * 2) in
  { raw0= Bigstringaf.sub raw ~off:0 ~len:weight
  ; raw1= Bigstringaf.sub raw ~off:weight ~len:weight
  ; flip= false }

let get_payload { raw0; raw1; flip; } =
  if flip then raw0 else raw1

let get_source { raw0; raw1; flip; } =
  if flip then raw1 else raw0

let flip t = { t with flip = not t.flip }
let raw { raw; _ } = get_payload raw
let len { len; _ } = len

let uncompress
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> [ `A | `B | `C | `D ] -> raw -> cursor:int -> W.slice -> (v, s) io
  = fun ({ bind; return; } as s) ~map t kind raw ~cursor slice ->
    let ( >>= ) = bind in

    let l = ref 0 in
    let p = ref false in
    let o = get_payload raw in
    let decoder = Zz.M.decoder `Manual ~o ~allocate:t.allocate in

    let rec go cursor decoder = match Zz.M.decode decoder with
      | `Malformed err -> failwith err
      | `End decoder ->
        let len = bigstring_length o - Zz.M.dst_rem decoder in
        assert (len = 0) ;
        assert (!p) ;
        (* XXX(dinosaure): we gave a [o] buffer which is enough to store
           inflated data. At the end, [decoder] should not return more than one
           [`Flush]. *)
        return { kind; raw; len= !l; }
      | `Flush decoder ->
        assert (not !p) ; p := true ;
        l := bigstring_length o - Zz.M.dst_rem decoder ;
        let decoder = Zz.M.flush decoder in
        (go[@tailcall]) cursor decoder
      | `Await decoder ->
        W.load s ~map t.ws cursor >>= function
        | Some slice ->
          let decoder = Zz.M.src decoder slice.W.payload 0 slice.W.length in
          (go[@tailcall]) (slice.W.offset + slice.W.length) decoder
        | None ->
          let decoder = Zz.M.src decoder Dd.bigstring_empty 0 0 in
          (go[@tailcall]) cursor decoder in
    let off = cursor - slice.W.offset in
    let len = slice.W.length - off in
    let decoder = Zz.M.src decoder slice.W.payload off len in
    go (slice.W.offset + slice.W.length) decoder

let of_delta
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> [ `A | `B | `C | `D ] -> raw -> cursor:int -> W.slice -> (v, s) io
  = fun ({ bind; return; } as s) ~map t kind raw ~cursor slice ->
    let ( >>= ) = bind in

    let l = ref 0 in
    let decoder = Zh.M.decoder ~o:t.tmp ~allocate:t.allocate `Manual in

    let rec go cursor decoder = match Zh.M.decode decoder with
      | `End _ -> return { kind; raw; len= !l; }
      | `Malformed err -> failwith err
      | `Header (src_len, dst_len, decoder) ->
        let source = get_source raw in
        let payload = get_payload raw in

        l := dst_len ;
        assert (bigstring_length source >= src_len) ;
        assert (bigstring_length payload >= dst_len) ;

        let decoder = Zh.M.dst decoder payload 0 dst_len in
        let decoder = Zh.M.source decoder source in
        (go[@tailcall]) cursor decoder
      | `Await decoder ->
        W.load s ~map t.ws cursor >>= function
        | None ->
          let decoder = Zh.M.src decoder Dd.bigstring_empty 0 0 in
          (go[@tailcall]) cursor decoder
        | Some slice ->
          let decoder = Zh.M.src decoder slice.W.payload 0 slice.W.length in
          (go[@tailcall]) (slice.W.offset + slice.W.length) decoder in
    let off = cursor - slice.W.offset in
    let len = slice.W.length - off in
    let decoder = Zh.M.src decoder slice.W.payload off len in
    go (slice.W.offset + slice.W.length) decoder

let rec of_ofs_delta
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> raw -> anchor:int -> cursor:int -> W.slice -> (v, s) io
  = fun ({ bind; _ } as s) ~map t raw ~anchor ~cursor slice ->
    let ( >>= ) = bind in

    header_of_ofs_detla s ~map t cursor slice >>= fun (base_offset, pos, slice) ->
    of_offset s ~map t (flip raw) ~cursor:(anchor - base_offset) >>= fun v ->
    of_delta s ~map t v.kind raw ~cursor:(slice.W.offset + pos) slice

and of_ref_delta
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> raw -> cursor:int -> W.slice -> (v, s) io
  = fun ({ bind; _ } as s) ~map t raw ~cursor slice ->
  let ( >>= ) = bind in

  header_of_ref_delta s ~map t cursor slice >>= fun (uid, pos, slice) ->
  of_uid s ~map t (flip raw) uid >>= fun v ->
  of_delta s ~map t v.kind raw ~cursor:(slice.W.offset + pos) slice

and of_uid
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> raw -> Uid.t -> (v, s) io
  = fun s ~map t raw uid ->
    let cursor = t.fd uid in
    of_offset s ~map t raw ~cursor

and of_offset
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> raw -> cursor:int -> (v, s) io
  = fun ({ bind; _ } as s) ~map t raw ~cursor ->
    let ( >>= ) = bind in
    W.load s ~map t.ws cursor >>= function
    | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_offset])" cursor
    | Some slice ->
      header_of_entry s ~map t cursor slice >>= fun (kind, _, pos, slice) ->

      match kind with
      | 0b000 | 0b101 -> failwith "bad type"
      | 0b001 ->
        uncompress s ~map t `A raw ~cursor:(slice.W.offset + pos) slice
      | 0b010 ->
        uncompress s ~map t `B raw ~cursor:(slice.W.offset + pos) slice
      | 0b011 ->
        uncompress s ~map t `C raw ~cursor:(slice.W.offset + pos) slice
      | 0b100 ->
        uncompress s ~map t `D raw ~cursor:(slice.W.offset + pos) slice
      | 0b110 ->
        of_ofs_delta s ~map t raw ~anchor:cursor ~cursor:(slice.W.offset + pos) slice
      | 0b111 ->
        of_ref_delta s ~map t raw ~cursor:(slice.W.offset + pos) slice
      | _ -> assert false

type path =
  { path : int array
  ; depth : int }

let pp_path ppf { depth; path; } =
  Fmt.pf ppf "  %d@\n" path.(0) ;

  for i = 1 to depth - 1 do
    Fmt.pf ppf "Δ %d@\n" path.(i)
  done

let rec fill_path_from_ofs_delta
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> depth:int -> int array -> anchor:int -> cursor:int -> W.slice -> (int, s) io
  = fun ({ bind; _ } as s) ~map t ~depth path ~anchor ~cursor slice ->
    let ( >>= ) = bind in

    header_of_ofs_detla s ~map t cursor slice >>= fun (base_offset, _, _) ->
    (fill_path_from_offset[@tailcall]) s ~map t ~depth:(succ depth) path ~cursor:(anchor - base_offset)

and fill_path_from_ref_delta
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> depth:int -> int array -> cursor:int -> W.slice -> (int, s) io
  = fun ({ bind; _ } as s) ~map t ~depth path ~cursor slice ->
  let ( >>= ) = bind in

  header_of_ref_delta s ~map t cursor slice >>= fun (uid, _, _) ->
  (fill_path_from_uid[@tailcall]) s ~map t ~depth path uid

and fill_path_from_uid
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> depth:int -> int array -> Uid.t -> (int, s) io
  = fun s ~map t ~depth path uid ->
    let cursor = t.fd uid in
    path.(depth - 1) <- cursor ;
    (fill_path_from_offset[@tailcall]) s ~map t ~depth:(succ depth) path ~cursor

and fill_path_from_offset
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> depth:int -> int array -> cursor:int -> (int, s) io
  = fun ({ return; bind; } as s) ~map t ~depth path ~cursor ->
    let ( >>= ) = bind in
    W.load s ~map t.ws cursor >>= function
    | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_offset])" cursor
    | Some slice ->

      path.(depth - 1) <- cursor ;
      header_of_entry s ~map t cursor slice >>= fun (kind, _, pos, slice) ->

      match kind with
      | 0b000 | 0b101 -> failwith "bad type"
      | 0b001 | 0b010 | 0b011 | 0b100 ->
        return depth
      | 0b110 ->
        (fill_path_from_ofs_delta[@tailcall]) s ~map t ~depth path ~anchor:cursor ~cursor:(slice.W.offset + pos) slice
      | 0b111 ->
        (fill_path_from_ref_delta[@tailcall]) s ~map t ~depth path ~cursor:(slice.W.offset + pos) slice
      | _ -> assert false

let path_of_offset
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> cursor:int -> (path, s) io
  = fun ({ return; bind; } as s) ~map t ~cursor ->
    let ( >>= ) = bind in
    let path = Array.make 50 0 in
    fill_path_from_offset s ~map t ~depth:1 path ~cursor >>= fun depth ->
    return { depth; path; }

let of_offset_with_source
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> [ `A | `B | `C | `D ] -> raw -> cursor:int -> (v, s) io
  = fun ({ bind; _ } as s) ~map t kind raw ~cursor ->
    let ( >>= ) = bind in
    W.load s ~map t.ws cursor >>= function
    | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_offset])" cursor
    | Some slice ->
      header_of_entry s ~map t cursor slice >>= fun (hdr, _, pos, slice) ->

      match hdr with
      | 0b000 | 0b101 -> failwith "bad type"
      | 0b001 ->
        assert (kind = `A) ;
        uncompress s ~map t `A raw ~cursor:(slice.W.offset + pos) slice
      | 0b010 ->
        assert (kind = `B) ;
        uncompress s ~map t `B raw ~cursor:(slice.W.offset + pos) slice
      | 0b011 ->
        assert (kind = `C) ;
        uncompress s ~map t `C raw ~cursor:(slice.W.offset + pos) slice
      | 0b100 ->
        assert (kind = `D) ;
        uncompress s ~map t `D raw ~cursor:(slice.W.offset + pos) slice
      | 0b110 ->
        let cursor = slice.W.offset + pos in
        header_of_ofs_detla s ~map t cursor slice >>= fun (_, pos, slice) ->
        of_delta s ~map t kind raw ~cursor:(slice.W.offset + pos) slice
      | 0b111 ->
        let cursor = slice.W.offset + pos in
        header_of_ref_delta s ~map t cursor slice >>= fun (_, pos, slice) ->
        of_delta s ~map t kind raw ~cursor:(slice.W.offset + pos) slice
      | _ -> assert false

let base_of_offset
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> raw -> cursor:int -> (v, s) io
  = fun ({ bind; _ } as s) ~map t raw ~cursor ->
    let ( >>= ) = bind in
    W.load s ~map t.ws cursor >>= function
    | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_offset])" cursor
    | Some slice ->
      header_of_entry s ~map t cursor slice >>= fun (hdr, _, pos, slice) ->
      let kind = match hdr with
        | 0b001 -> `A | 0b010 -> `B | 0b011 -> `C | 0b100 -> `D | _ -> assert false in
      uncompress s ~map t kind raw ~cursor:(slice.W.offset + pos) slice

let base_of_path { depth; path; } = path.(depth - 1)

let of_offset_with_path
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> path:path -> raw -> cursor:int -> (v, s) io
  = fun ({ bind; return; } as s) ~map t ~path raw ~cursor ->
    assert (cursor == path.path.(0)) ;
    let ( >>= ) = bind in

    base_of_offset s ~map t raw ~cursor:(base_of_path path) >>= fun base ->
    let rec go depth raw =
      of_offset_with_source s ~map t base.kind raw ~cursor:path.path.(depth - 1) >>= fun v ->
      if depth == 1
      then return v
      else (go[@tailcall]) (pred depth) (flip raw) in
    if path.depth > 1 then go (path.depth - 1) (flip raw) else return base
