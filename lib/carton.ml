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

(* XXX(dinosaure): [weight_of_*] wants to know the biggest object into the path
   patch. The goal then is to allocate two times the biggest object and apply
   recursively objects each others to reconstruct the final object. *)

let weight_of_delta
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> int -> W.slice -> (int, s) io
  = fun ({ return; bind; } as s) ~map t ro slice ->
    let ( >>= ) = bind in
    let decoder = Zh.M.decoder ~o:t.tmp ~allocate:t.allocate `Manual in
    let rec go ro decoder = match Zh.M.decode decoder with
      | `End _ -> assert false (* XXX(dinosaure): [`End] never appears before [`Header]. *)
      | `Malformed err -> failwith err
      | `Header (src_len, dst_len, _) -> return (max src_len dst_len)
      | `Await decoder ->
        W.load s ~map t.ws ro >>= function
        | None ->
          let decoder = Zh.M.src decoder Dd.bigstring_empty 0 0 in
          (* XXX(dinosaure): End of stream, [Zh] should return [`Malformed] then. *)
          go ro decoder
        | Some slice ->
          let decoder = Zh.M.src decoder slice.W.payload 0 slice.W.length in
          go (slice.W.offset + slice.W.length) decoder in
    let off = ro - slice.W.offset in
    let len = slice.W.length - off in
    let decoder = Zh.M.src decoder slice.W.payload off len in
    go (slice.W.offset + slice.W.length) decoder

let rec weight_of_ref_delta
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> int -> W.slice -> (int, s) io
  = fun ({ bind; return; } as s) ~map t ro slice ->
  let ( >>= ) = bind in
  let payload = ref slice.W.payload in
  let offset = ref slice.W.offset in
  let length = ref slice.W.length in

  let i_pos = ref (ro - slice.W.offset) in
  let i_rem = slice.W.length - !i_pos in

  let junk =
    if i_rem >= Uid.length
    then return (fun () -> incr i_pos)
    else W.load s ~map t.ws (!offset + !length) >>= function
      | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_ref_delta])" (!offset + !length)
      | Some next_slice ->
        return (fun () -> incr i_pos
                        ; if !i_pos == !length
                          then ( assert (!payload != next_slice.W.payload)
                               ; i_pos := 0
                               ; payload := next_slice.W.payload
                               ; offset := next_slice.W.offset
                               ; length := next_slice.W.length )) in
  junk >>= fun junk ->

  let uid =
    if i_rem >= Uid.length
    then
      let uid = Bigstringaf.substring slice.W.payload ~off:!i_pos ~len:Uid.length in
      let uid = Uid.of_raw_string uid in
      for _ = 0 to Uid.length - 1 do junk () done ; uid
    else
      let uid = Bytes.create Uid.length in
      for i = 0 to Uid.length - 1 do
        Bytes.unsafe_set uid i (unsafe_get_char !payload !i_pos) ;
        junk ()
      done ; Uid.of_raw_string (Bytes.unsafe_to_string uid) in

  weight_of_delta s ~map t (slice.W.offset + !i_pos) slice >>= fun weight ->
  weight_of_uid s ~map t uid >>= fun weight_of_source ->
  return (max weight_of_source weight)


and weight_of_ofs_delta
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> int -> int -> W.slice -> (int, s) io
  = fun ({ bind; return; } as s) ~map t anchor ro slice ->
    let ( >>= ) = bind in
    let slice = ref slice in
    let i_pos = ref (ro - !slice.W.offset) in
    let i_rem = !slice.W.length - !i_pos in

    (* XXX(dinosaure): header with [offset] part can not be bigger than 10 bytes,
       even if it's a variable-length. *)

    let junk =
      if i_rem >= 10
      then return (fun () -> incr i_pos)
      else W.load s ~map t.ws (!slice.W.offset + !slice.W.length) >>= function
        | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_ofs_delta])" (!slice.W.offset + !slice.W.length)
        | Some next_slice ->
          return (fun () -> incr i_pos
                          ; if !i_pos == !slice.W.length
                            then ( assert (!slice != next_slice)
                                 ; i_pos := 0
                                 ; slice := next_slice )) in
    junk >>= fun junk ->
    let c = ref (unsafe_get_uint8 !slice.W.payload !i_pos) in
    junk () ;
    let base_offset = ref (!c land 127) in

    while !c land 128 != 0
    do
      incr base_offset ;
      c := unsafe_get_uint8 !slice.W.payload !i_pos ;
      junk () ;
      base_offset := (!base_offset lsl 7) + (!c land 127) ;
    done ;

    weight_of_delta s ~map t (!slice.W.offset + !i_pos) !slice >>= fun weight ->
    weight_of_offset s ~map t (anchor - !base_offset) >>= fun weight_of_source ->
    return (max weight_of_source weight)

and weight_of_uid
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> Uid.t -> (int, s) io
  = fun s ~map t uid ->
    let ro = t.fd uid in
    weight_of_offset s ~map t ro

and weight_of_offset
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> int -> (int, s) io
  = fun ({ bind; return; } as s) ~map t ro ->
    let ( >>= ) = bind in
    W.load s ~map t.ws ro >>= function
    | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_offset])" ro
    | Some ({ W.payload; offset; length; } as slice) ->
      let payload = ref payload in
      let offset = ref offset in
      let length = ref length in

      let i_pos = ref (ro - !offset) in
      let i_rem = !length - !i_pos in

      (* XXX(dinosaure): header with [len] part can not be bigger than 10 bytes,
         even if it's a variable-length. *)

      let junk =
        if i_rem >= 10
        then return (fun () -> incr i_pos)
        else W.load s ~map t.ws (!offset + !length) >>= function
          | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_ofs_delta])" (!offset + !length)
          | Some next_slice ->
            return (fun () -> incr i_pos
                            ; if !i_pos == !length
                              then ( assert (!payload != next_slice.W.payload)
                                   ; i_pos := 0
                                   ; payload := next_slice.W.payload
                                   ; offset := next_slice.W.offset
                                   ; length := next_slice.W.length )) in
      junk >>= fun junk ->
      let c = ref (unsafe_get_uint8 !payload !i_pos) in
      junk () ;
      let kind = (!c asr 4) land 7 in
      let size = ref (!c land 15) in
      let shft = ref 4 in

      while !c land 0x80 != 0
      do
        c := unsafe_get_uint8 !payload !i_pos ;
        incr i_pos ;
        size := !size + ((!c land 0x7f) lsl !shft) ;
        shft := !shft + 7 ;
      done ;

      match kind with
      | 0b000 | 0b101 -> failwith "bad type"
      | 0b001 | 0b010 | 0b011 | 0b100 -> return !size
      | 0b110 -> weight_of_ofs_delta s ~map t ro (!offset + !i_pos) slice
      | 0b111 -> weight_of_ref_delta s ~map t (!offset + !i_pos) slice
      | _ -> assert false

type raw =
  { raw0 : bigstring
  ; raw1 : bigstring
  ; flip : bool }

type v =
  { kind : [ `A | `B | `C | `D ]
  ; raw  : raw
  ; len  : int }

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

let uncompress
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> [ `A | `B | `C | `D ] -> raw -> int -> W.slice -> (v, s) io
  = fun ({ bind; return; } as s) ~map t kind raw ro slice ->
    let ( >>= ) = bind in

    let l = ref 0 in
    let p = ref false in
    let o = get_payload raw in
    let decoder = Zz.M.decoder `Manual ~o ~allocate:t.allocate in

    let rec go ro decoder = match Zz.M.decode decoder with
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
        go ro decoder
      | `Await decoder ->
        W.load s ~map t.ws ro >>= function
        | Some slice ->
          let decoder = Zz.M.src decoder slice.W.payload 0 slice.W.length in
          go (slice.W.offset + slice.W.length) decoder
        | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_ofs_delta])" (slice.W.offset + slice.W.length) in
    let off = ro - slice.W.offset in
    let len = slice.W.length - off in
    let decoder = Zz.M.src decoder slice.W.payload off len in
    go (slice.W.offset + slice.W.length) decoder

let of_delta
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> [ `A | `B | `C | `D ] -> raw -> int -> W.slice -> (v, s) io
  = fun ({ bind; return; } as s) ~map t kind raw ro slice ->
    let ( >>= ) = bind in
    let l = ref 0 in
    let decoder = Zh.M.decoder ~o:t.tmp ~allocate:t.allocate `Manual in
    let rec go ro decoder = match Zh.M.decode decoder with
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
        go ro decoder
      | `Await decoder ->
        W.load s ~map t.ws ro >>= function
        | None ->
          let decoder = Zh.M.src decoder Dd.bigstring_empty 0 0 in
          (* XXX(dinosaure): End of stream, [Zh] should return [`Malformed] then. *)
          go ro decoder
        | Some slice ->
          let decoder = Zh.M.src decoder slice.W.payload 0 slice.W.length in
          go (slice.W.offset + slice.W.length) decoder in
    let off = ro - slice.W.offset in
    let len = slice.W.length - off in
    let decoder = Zh.M.src decoder slice.W.payload off len in
    go (slice.W.offset + slice.W.length) decoder

let rec of_ofs_delta
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> raw -> int -> int -> W.slice -> (v, s) io
  = fun ({ bind; return; } as s) ~map t raw anchor ro slice ->
    let ( >>= ) = bind in
    let slice = ref slice in
    let i_pos = ref (ro - !slice.W.offset) in
    let i_rem = !slice.W.length - !i_pos in

    (* XXX(dinosaure): header with [offset] part can not be bigger than 10 bytes,
       even if it's a variable-length. *)

    let junk =
      if i_rem >= 10
      then return (fun () -> incr i_pos)
      else W.load s ~map t.ws (!slice.W.offset + !slice.W.length) >>= function
        | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_ofs_delta])" (!slice.W.offset + !slice.W.length)
        | Some next_slice ->
          return (fun () -> incr i_pos
                          ; if !i_pos == !slice.W.length
                            then ( assert (!slice != next_slice)
                                 ; i_pos := 0
                                 ; slice := next_slice )) in
    junk >>= fun junk ->
    let c = ref (unsafe_get_uint8 !slice.W.payload !i_pos) in junk () ;
    let base_offset = ref (!c land 127) in

    while !c land 128 != 0
    do
      incr base_offset ;
      c := unsafe_get_uint8 !slice.W.payload !i_pos ; junk () ;
      base_offset := (!base_offset lsl 7) + (!c land 127) ;
    done ;

    of_offset s ~map t (flip raw) (anchor - !base_offset) >>= fun v ->
    of_delta s ~map t v.kind raw (!slice.W.offset + !i_pos) !slice

and of_offset
  : type fd s. s scheduler -> map:(fd, s) W.map -> fd t -> raw -> int -> (v, s) io
  = fun ({ bind; return; } as s) ~map t raw ro ->
    let ( >>= ) = bind in
    W.load s ~map t.ws ro >>= function
    | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_offset])" ro
    | Some slice ->
      let slice = ref slice in
      let i_pos = ref (ro - !slice.W.offset) in
      let i_rem = !slice.W.length - !i_pos in

      (* XXX(dinosaure): header with [len] part can not be bigger than 10 bytes,
         even if it's a variable-length. *)

      let junk =
        if i_rem >= 10
        then return (fun () -> incr i_pos)
        else W.load s ~map t.ws (!slice.W.offset + !slice.W.length) >>= function
          | None ->
            Fmt.failwith "Reach end of pack (ask: %d, [weight_of_ofs_delta])" (!slice.W.offset + !slice.W.length)
          | Some next_slice ->
            return (fun () -> incr i_pos
                            ; if !i_pos == !slice.W.length
                              then ( assert (!slice.W.payload != next_slice.W.payload)
                                   ; i_pos := 0
                                   ; slice := next_slice )) in
      junk >>= fun junk ->
      let c = ref (unsafe_get_uint8 !slice.W.payload !i_pos) in
      junk () ;
      let kind = (!c asr 4) land 7 in
      let size = ref (!c land 15) in
      let shft = ref 4 in

      while !c land 0x80 != 0
      do
        c := unsafe_get_uint8 !slice.W.payload !i_pos ;
        incr i_pos ;
        size := !size + ((!c land 0x7f) lsl !shft) ;
        shft := !shft + 7 ;
      done ;

      match kind with
      | 0b000 | 0b101 -> failwith "bad type"
      | 0b001 ->
        uncompress s ~map t `A raw (!slice.W.offset + !i_pos) !slice
      | 0b010 ->
        uncompress s ~map t `B raw (!slice.W.offset + !i_pos) !slice
      | 0b011 ->
        uncompress s ~map t `C raw (!slice.W.offset + !i_pos) !slice
      | 0b100 ->
        uncompress s ~map t `D raw (!slice.W.offset + !i_pos) !slice
      | 0b110 ->
        of_ofs_delta s ~map t raw ro (!slice.W.offset + !i_pos) !slice
      | 0b111 -> assert false
      | _ -> assert false
