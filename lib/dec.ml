open Sigs

let () = Printexc.record_backtrace true

let input_bigstring ic buf off len =
  let tmp = Bytes.create len in
  let len = input ic tmp 0 len in
  Bigstringaf.blit_from_bytes tmp ~src_off:0 buf ~dst_off:off ~len ; len

module Idx = Idx

type ('fd, 's) read = 'fd -> bytes -> off:int -> len:int -> (int, 's) io

module Fp (Uid : UID) = struct
  type src = [ `Channel of in_channel | `String of string | `Manual ]
  type optint = Optint.t

  type nonrec kind =
    | Base of kind
    | Ofs of { sub : int; source : int; target : int; }
    | Ref of { ptr : Uid.t; source : int; target : int; }

  type decoder =
    { src : src
    ; i : Bigstringaf.t
    ; i_pos : int
    ; i_len : int
    ; n : int (* number of objects *)
    ; c : int (* counter of objects *)
    ; v : int (* version of PACK file *)
    ; r : int (* how many bytes consumed *)
    ; s : s
    ; o : Bigstringaf.t
    ; t_tmp : Bigstringaf.t
    ; t_len : int
    ; t_need : int
    ; t_peek : int
    ; ctx : Uid.ctx
    ; z : Zz.M.decoder
    ; k : decoder -> decode }
  and s =
    | Header | Entry | Inflate of entry | Hash
  and decode =
    [ `Await of decoder
    | `Peek of decoder
    | `Entry of (entry * decoder)
    | `End of Uid.t
    | `Malformed of string ]
  and entry =
    { offset : int; kind : kind; size : int; consumed : int; crc : optint; }

  let with_source source entry = match entry.kind with
    | Ofs { sub; target; _ } -> { entry with kind= Ofs { sub; source; target; }; }
    | Ref { ptr; target; _ } -> { entry with kind= Ref { ptr; source; target; }; }
    | _ -> entry

  let source entry = match entry.kind with
    | Ofs { source; _ } | Ref { source; _ } -> source
    | _ -> assert false

  let target entry = match entry.kind with
    | Ofs { target; _ } | Ref { target; _ } -> target
    | _ -> assert false

  let with_target target entry = match entry.kind with
    | Ofs { sub; source; _ } -> { entry with kind= Ofs { sub; source; target; }; }
    | Ref { ptr; source; _ } -> { entry with kind= Ref { ptr; source; target; }; }
    | _ -> entry

  let i_rem d = d.i_len - d.i_pos + 1
  let number { n; _ } = n
  let version { v; _ } = v
  let count { c; _ } = c

  let is_inflate = function Inflate _ -> true | _ -> false

  let eoi d = { d with i= Bigstringaf.empty
                    ; i_pos= 0
                    ; i_len= min_int }

  let malformedf fmt = Fmt.kstrf (fun err -> `Malformed err) fmt

  let src d s j l =
    if (j < 0 || l < 0 || j + l > Bigstringaf.length s)
    then Fmt.invalid_arg "Source out of bounds" ;
    if (l == 0) then eoi d
    else
      let z = if is_inflate d.s then Zz.M.src d.z s j l else d.z in
      { d with i= s
            ; i_pos= j
            ; i_len= j + l - 1
            ; z }

  let refill k d = match d.src with
    | `String _ -> k (eoi d)
    | `Channel ic ->
      let res = input_bigstring ic d.i 0 (Bigstringaf.length d.i) in
      k (src d d.i 0 res)
    | `Manual -> `Await { d with k }

  let rec peek k d = match d.src with
    | `String _ -> malformedf "Unexpected end of input"
    | `Channel ic ->
      let rem = i_rem d in

      if rem < d.t_peek
      then
        ( Bigstringaf.blit d.i ~src_off:d.i_pos d.i ~dst_off:0 ~len:rem ; (* compress *)
          let res = input_bigstring ic d.i rem (Bigstringaf.length d.i - rem) in
          peek k (src d d.i 0 (rem + res)) )
      else k d
    | `Manual ->
      let rem = i_rem d in

      if rem < d.t_peek
      then ( Bigstringaf.blit d.i ~src_off:d.i_pos d.i ~dst_off:0 ~len:rem ; (* compress *)
             `Peek { d with k= peek k; i_pos= 0; i_len= rem - 1; } )
      else k d

  let t_need d n = { d with t_need= n }
  let t_peek d n = { d with t_peek= n }

  let rec t_fill k d =
    let blit d len =
      Bigstringaf.blit d.i ~src_off:d.i_pos d.t_tmp ~dst_off:d.t_len ~len ;
      { d with i_pos= d.i_pos + len; r= d.r + len
            ; t_len= d.t_len + len } in
    let rem = i_rem d in
    if rem < 0 then malformedf "Unexpected end of input"
    else
      let need = d.t_need - d.t_len in
      if rem < need
      then let d = blit d rem in refill (t_fill k) d
      else let d = blit d need in k { d with t_need= 0 }

  let variable_length buf off top =
    let p = ref off in
    let i = ref 0 in
    let len = ref 0 in

    while ( let cmd = Char.code (Bigstringaf.get buf !p) in
            incr p
          ; len := !len lor ((cmd land 0x7f) lsl !i)
          ; i := !i + 7
          ; cmd land 0x80 != 0 && !p <= top )
    do () done ; (!p - off, !len)

  external get_int32 : bytes -> int -> int32 = "%caml_bytes_get32"
  external swap32 : int32 -> int32 = "%bswap_int32"

  let get_int32_be =
    if Sys.big_endian
    then fun buf off -> get_int32 buf off
    else fun buf off -> swap32 (get_int32 buf off)

  let check_header
    : type fd s. s scheduler -> (fd, s) read -> fd -> (int, s) io
    = fun { bind; return; } read fd ->
      let ( >>= ) = bind in
      let tmp = Bytes.create 12 in
      read fd tmp ~off:0 ~len:12 >>= fun n ->
      if n != 12 then Fmt.invalid_arg "Invalid PACK file" ;
      let h = get_int32_be tmp 0 in
      let v = get_int32_be tmp 4 in
      let n = get_int32_be tmp 8 in
      if h <> 0x5041434bl then Fmt.invalid_arg "Invalid PACK file (header: %lx <> %lx)" h 0x5041434bl ;
      if v <> 0x2l then Fmt.invalid_arg "Invalid version of PACK file" ;
      return (Int32.to_int n)

  let rec decode d = match d.s with
    | Header ->
      let refill_12 k d =
        if i_rem d >= 12
        then k d.i d.i_pos { d with i_pos= d.i_pos + 12; r= d.r + 12 }
        else t_fill (k d.t_tmp 0) (t_need d 12) in
      let k buf off d =
        let _ = Bigstringaf.get_int32_be buf off in
        let v = Bigstringaf.get_int32_be buf (off + 4) |> Int32.to_int in
        let n = Bigstringaf.get_int32_be buf (off + 8) |> Int32.to_int in
        if d.c == n
        then decode { d with v; n; s= Hash; k= decode; ctx= Uid.feed d.ctx buf ~off ~len:12 }
        else decode { d with v; n; s= Entry; k= decode; ctx= Uid.feed d.ctx buf ~off ~len:12 } in
      refill_12 k d
    | Entry ->
      let peek_10 k d = peek k (t_peek d 10) in

      let k_ofs_header crc offset size d =
        let p = ref d.i_pos in
        let c = ref (Char.code (Bigstringaf.get d.i !p)) in
        incr p ;
        let base_offset = ref (!c land 127) in

        while !c land 128 != 0
        do
          incr base_offset ;
          c := Char.code (Bigstringaf.get d.i !p) ;
          incr p ;
          base_offset := (!base_offset lsl 7) + (!c land 127) ;
        done ;

        let z = Zz.M.reset d.z in
        let z = Zz.M.src z d.i !p (i_rem { d with i_pos= !p }) in
        let crc = Checkseum.Crc32.digest_bigstring d.i d.i_pos (!p - d.i_pos) crc in
        let e = { offset; kind= Ofs { sub= !base_offset; source= (-1); target= (-1); }; size; consumed= 0; crc; } in

        decode { d with i_pos= !p; r= d.r + (!p - d.i_pos); c= succ d.c; z
                      ; s= Inflate e; k= decode
                      ; ctx= Uid.feed d.ctx d.i ~off:d.i_pos ~len:(!p - d.i_pos) } in

      let k_header d =
        let p = ref d.i_pos in
        let c = ref (Char.code (Bigstringaf.get d.i !p)) in
        incr p ;
        let kind = (!c asr 4) land 7 in
        let size = ref (!c land 15) in
        let shft = ref 4 in

        while !c land 0x80 != 0
        do
          c := Char.code (Bigstringaf.get d.i !p) ;
          incr p ;
          size := !size + ((!c land 0x7f) lsl !shft) ;
          shft := !shft + 7 ;
        done ;

        match kind with
        | 0b000 | 0b101 -> malformedf "Invalid type"
        | 0b001 | 0b010 | 0b011 | 0b100 as kind ->
          let z = Zz.M.reset d.z in
          let z = Zz.M.src z d.i !p (i_rem { d with i_pos= !p }) in
          let k = match kind with 0b001 -> `A | 0b010 -> `B | 0b011 -> `C | 0b100 -> `D | _ -> assert false in
          let crc = Checkseum.Crc32.digest_bigstring d.i d.i_pos (!p - d.i_pos) Checkseum.Crc32.default in
          let e = { offset= d.r; kind= Base k; size= !size; consumed= 0; crc; } in

          decode { d with i_pos= !p; r= d.r + (!p - d.i_pos); c= succ d.c; z
                        ; s= Inflate e; k= decode
                        ; ctx= Uid.feed d.ctx d.i ~off:d.i_pos ~len:(!p - d.i_pos) }
        | 0b110 ->
          let offset = d.r in
          let crc = Checkseum.Crc32.digest_bigstring d.i d.i_pos (!p - d.i_pos) Checkseum.Crc32.default in

          peek_10 (k_ofs_header crc offset !size)
            { d with i_pos= !p; r= d.r + (!p - d.i_pos)
                   ; ctx= Uid.feed d.ctx d.i ~off:d.i_pos ~len:(!p - d.i_pos) }
        | _ -> assert false in
      peek_10 k_header d
    | Inflate ({ kind= Base _; crc; _ } as entry) ->
      let rec go z = match Zz.M.decode z with
        | `Await z ->
          let len = i_rem d - Zz.M.src_rem z in
          let crc = Checkseum.Crc32.digest_bigstring d.i d.i_pos len crc in
          refill decode { d with z; i_pos= d.i_pos + len; r= d.r + len
                              ; s= Inflate { entry with crc }
                              ; ctx= Uid.feed d.ctx d.i ~off:d.i_pos ~len }
        | `Flush z ->
          go (Zz.M.flush z)
        | `Malformed _ as err -> err
        | `End z ->
          let len = i_rem d - Zz.M.src_rem z in
          let crc = Checkseum.Crc32.digest_bigstring d.i d.i_pos len crc in
          let z = Zz.M.reset z in
          let decoder = { d with i_pos= d.i_pos + len; r= d.r + len
                              ; z; s= if d.c == d.n then Hash else Entry
                              ; k= decode
                              ; ctx= Uid.feed d.ctx d.i ~off:d.i_pos ~len } in
          let entry = { entry with consumed= decoder.r - entry.offset; crc; } in
          `Entry (entry, decoder) in
      go d.z
    | Inflate ({ kind= (Ofs _ | Ref _); crc; _ } as entry) ->
      let source = ref (source entry) in
      let target = ref (target entry) in
      let first = ref (!source = (-1) && !target = (-1)) in

      let rec go z = match Zz.M.decode z with
        | `Await z ->
          let len = i_rem d - Zz.M.src_rem z in
          let crc = Checkseum.Crc32.digest_bigstring d.i d.i_pos len crc in
          let entry = with_source !source entry in
          let entry = with_target !target entry in
          refill decode { d with z; i_pos= d.i_pos + len; r= d.r + len
                              ; s= Inflate { entry with crc }
                              ; ctx= Uid.feed d.ctx d.i ~off:d.i_pos ~len }
        | `Flush z ->
          if !first
          then ( let len = Bigstringaf.length d.o - Zz.M.dst_rem z in
                let x, src_len = variable_length d.o 0 len in
                let _, dst_len = variable_length d.o x len in
                source := src_len ; target := dst_len ; first := false ) ;

          go (Zz.M.flush z)
        | `Malformed _ as err -> err
        | `End z ->
          if !first
          then ( let len = Bigstringaf.length d.o - Zz.M.dst_rem z in
                let x, src_len = variable_length d.o 0 len in
                let _, dst_len = variable_length d.o x len in
                source := src_len ; target := dst_len ; first := false ) ;

          let len = i_rem d - Zz.M.src_rem z in
          let crc = Checkseum.Crc32.digest_bigstring d.i d.i_pos len crc in
          let z = Zz.M.reset z in
          let decoder = { d with i_pos= d.i_pos + len; r= d.r + len
                              ; z; s= if d.c == d.n then Hash else Entry
                              ; k= decode
                              ; ctx= Uid.feed d.ctx d.i ~off:d.i_pos ~len } in
          let entry = { entry with crc; consumed= decoder.r - entry.offset } in
          let entry = with_source !source entry in
          let entry = with_target !target entry in
          `Entry (entry, decoder) in
      go d.z
    | Hash ->
      let refill_uid k d =
        if i_rem d >= Uid.length
        then k d.i d.i_pos { d with i_pos= d.i_pos + Uid.length; r= d.r + Uid.length }
        else t_fill (k d.t_tmp 0) (t_need d Uid.length) in
      let k buf off d =
        let expect = Uid.of_raw_string (Bigstringaf.substring buf ~off ~len:Uid.length) in
        let have = Uid.get d.ctx in

        if Uid.equal expect have
        then `End have
        else malformedf "Unexpected hash: %a <> %a"
            Uid.pp expect Uid.pp have in
      refill_uid k d

  let decoder ~o ~allocate src =
    let i, i_pos, i_len = match src with
      | `Manual -> Bigstringaf.empty, 1, 0
      | `String x -> Bigstringaf.of_string x ~off:0 ~len:(String.length x), 0, String.length x - 1
      | `Channel _ -> Bigstringaf.create Zz.io_buffer_size, 1, 0 in
    { src
    ; i; i_pos; i_len
    ; n= 0
    ; c= 0
    ; v= 0
    ; r= 0
    ; o
    ; s= Header
    ; t_tmp= Bigstringaf.create Uid.length
    ; t_len= 0
    ; t_need= 0
    ; t_peek= 0
    ; ctx= Uid.empty
    ; z= Zz.M.decoder `Manual ~o ~allocate
    ; k= decode }

  let decode d = d.k d
end

module W = struct
  type 'fd t =
    { mutable cur : int
    ; w : slice Weak.t
    ; m : int
    ; fd : 'fd }
  and slice = { offset : int
              ; length : int
              ; payload : Bigstringaf.t }
  and ('fd, 's) map = 'fd -> pos:int -> int -> (Bigstringaf.t, 's) io

  let make fd =
    { cur= 0
    ; w= Weak.create (0xffff + 1)
    ; m= 0xffff
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
                       ; length= Bigstringaf.length payload
                       ; payload; } in
      Weak.set t.w (t.cur land 0xffff) slice ;
      t.cur <- t.cur + 1 ; return slice

  let load
    : type fd s. s scheduler -> map:(fd, s) map -> fd t -> int -> (slice option, s) io
    = fun ({ return; _ } as s) ~map t w ->
    let exception Found in
    let slice = ref None in
    try
      for i = 0 to Weak.length t.w - 1
      do match Weak.get t.w i with
          | Some ({ offset; length; _ } as s) ->
            if w >= offset && w < offset + length && length - (w - offset) >= 20
            (* XXX(dinosaure): when we want to load a new window, we need to see
               if we have, at least, 20 bytes between the given offset and the
               end of the window. Otherwise, we can return a window with 0 bytes
               available according the given offset. *)
            then ( slice := Some s ; raise_notrace Found )
          | None -> ()
      done ; heavy_load s ~map t w
    with Found -> return !slice
end

type ('fd, 'uid) t =
  { ws : 'fd W.t
  ; fd : 'uid -> int
  ; uid_ln : int
  ; uid_rw : string -> 'uid
  ; tmp : Bigstringaf.t
  ; allocate : int -> Zz.window }

let with_z tmp t = { t with tmp }
let with_w ws t = { t with ws }
let with_allocate ~allocate t = { t with allocate }
let fd { ws= { W.fd; _ }; _ } = fd

let make
  : type fd uid. fd -> z:Bigstringaf.t -> allocate:(int -> Zz.window) -> uid_ln:int -> uid_rw:(string -> uid) -> (uid -> int) -> (fd, uid) t
  = fun fd ~z ~allocate ~uid_ln ~uid_rw where ->
    { ws= W.make fd
    ; fd= where
    ; uid_ln; uid_rw
    ; tmp= z
    ; allocate }

type weight = int

let weight_of_int_exn x =
  if x < 0 then Fmt.invalid_arg "weight_of_int_exn"
  else x

let null = 0

let weight_of_delta
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> weight:weight -> cursor:int -> W.slice -> (weight, s) io
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
          let off = cursor - slice.W.offset in
          let len = slice.W.length - off in
          let decoder = Zh.M.src decoder slice.W.payload off len in
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
    if i_rem >= t.uid_ln
    then return (fun () -> incr i_pos)
    else
      W.load s ~map t.ws (!slice.W.offset + !slice.W.length) >>= function
      | None -> assert false
      | Some next_slice ->
        let consume () =
          incr i_pos ;
          if !i_pos == !slice.W.length
          then ( assert (!slice != next_slice)
               ; i_pos := (!slice.W.offset + !slice.W.length) - next_slice.W.offset
               ; slice := next_slice ) in
        return consume in
  fiber >>= fun consume ->

  let uid =
    if i_rem >= t.uid_ln
    then
      let uid = Bigstringaf.substring !slice.W.payload ~off:!i_pos ~len:t.uid_ln in
      let uid = t.uid_rw uid in
      for _ = 0 to t.uid_ln - 1 do consume () done ; uid
    else
      let uid = Bytes.create t.uid_ln in
      for i = 0 to t.uid_ln - 1 do
        Bytes.unsafe_set uid i (Bigstringaf.get !slice.W.payload !i_pos) ;
        consume ()
      done ; t.uid_rw (Bytes.unsafe_to_string uid) in

  return (uid, !i_pos, !slice)

let header_of_ofs_delta ({ bind; return; } as s) ~map t cursor slice =
  let ( >>= ) = bind in
  let slice = ref slice in
  let i_pos = ref (cursor - !slice.W.offset) in
  let i_rem = !slice.W.length - !i_pos in

  let fiber =
    if i_rem >= 10
    then return (fun () -> incr i_pos)
    else
      W.load s ~map t.ws (!slice.W.offset + !slice.W.length) >>= function
      | None -> assert false
      | Some next_slice ->
        let consume () =
          incr i_pos ;
          if !i_pos == !slice.W.length
          then ( assert (!slice != next_slice)
               ; i_pos := (!slice.W.offset + !slice.W.length) - next_slice.W.offset
               ; slice := next_slice ) in
        return consume in
  fiber >>= fun consume ->

  let c = ref (Char.code (Bigstringaf.get !slice.W.payload !i_pos)) in
  consume () ;
  let base_offset = ref (!c land 127) in

  while !c land 128 != 0
  do
    incr base_offset ;
    c := Char.code (Bigstringaf.get !slice.W.payload !i_pos) ;
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
    if i_rem >= 10
    then return (fun () -> incr i_pos)
    else
      W.load s ~map t.ws (!slice.W.offset + !slice.W.length) >>= function
      | None -> assert false
      | Some next_slice ->
        let consume () =
          incr i_pos ;
          if !i_pos == !slice.W.length
          then ( assert (!slice != next_slice)
               ; i_pos := (!slice.W.offset + !slice.W.length) - next_slice.W.offset
               ; slice := next_slice ) in
        return consume in
  fiber >>= fun consume ->

  let c = ref (Char.code (Bigstringaf.get !slice.W.payload !i_pos)) in
  consume () ;
  let kind = (!c asr 4) land 7 in
  let size = ref (!c land 15) in
  let shft = ref 4 in

  while !c land 0x80 != 0
  do
    c := Char.code (Bigstringaf.get !slice.W.payload !i_pos) ;
    consume () ;
    size := !size + ((!c land 0x7f) lsl !shft) ;
    shft := !shft + 7 ;
  done ;

  return (kind, !size, !i_pos, !slice)

let rec weight_of_ref_delta
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> weight:weight -> cursor:int -> W.slice -> (weight, s) io
  = fun ({ bind; _ } as s) ~map t ~weight ~cursor slice ->
    let ( >>= ) = bind in

    header_of_ref_delta s ~map t cursor slice >>= fun (uid, pos, slice) ->
    weight_of_delta s ~map t ~weight ~cursor:(slice.W.offset + pos) slice >>= fun weight ->
    (weight_of_uid[@tailcall]) s ~map t ~weight uid

and weight_of_ofs_delta
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> weight:weight -> anchor:int -> cursor:int -> W.slice -> (weight, s) io
  = fun ({ bind; _ } as s) ~map t ~weight ~anchor ~cursor slice ->
    let ( >>= ) = bind in

    header_of_ofs_delta s ~map t cursor slice >>= fun (base_offset, pos, slice) ->
    weight_of_delta s ~map t ~weight ~cursor:(slice.W.offset + pos) slice >>= fun weight ->
    (weight_of_offset[@tailcall]) s ~map t ~weight ~cursor:(anchor - base_offset)

and weight_of_uid
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> weight:weight -> uid -> (weight, s) io
  = fun s ~map t ~weight uid ->
    let cursor = t.fd uid in
    (weight_of_offset[@tailcall]) s ~map t ~weight ~cursor

and weight_of_offset
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> weight:weight -> cursor:int -> (weight, s) io
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
  { raw0 : Bigstringaf.t
  ; raw1 : Bigstringaf.t
  ; flip : bool }

type v =
  { kind : kind
  ; raw  : raw
  ; len  : int
  ; depth : int }

let v ~kind ?(depth= 1) raw =
  let len = Bigstringaf.length raw in
  { kind; raw= { raw0= raw; raw1= Bigstringaf.empty; flip= true; }; len; depth; }

let kind { kind; _ } = kind

let make_raw ~weight =
  let raw = Bigstringaf.create (weight * 2) in
  { raw0= Bigstringaf.sub raw ~off:0 ~len:weight
  ; raw1= Bigstringaf.sub raw ~off:weight ~len:weight
  ; flip= false }

let weight_of_raw { raw0; _ } = Bigstringaf.length raw0

let get_payload { raw0; raw1; flip; } =
  if flip then raw0 else raw1

let get_source { raw0; raw1; flip; } =
  if flip then raw1 else raw0

let flip t = { t with flip = not t.flip }
let raw { raw; _ } = get_payload raw
let len { len; _ } = len
let depth { depth; _ } = depth

let uncompress
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> kind -> raw -> cursor:int -> W.slice -> (v, s) io
  = fun ({ bind; return; } as s) ~map t kind raw ~cursor slice ->
    let ( >>= ) = bind in

    let l = ref 0 in
    let p = ref false in
    let o = get_payload raw in
    let decoder = Zz.M.decoder `Manual ~o ~allocate:t.allocate in

    let rec go cursor decoder = match Zz.M.decode decoder with
      | `Malformed err -> failwith err
      | `End decoder ->
        let len = Bigstringaf.length o - Zz.M.dst_rem decoder in
        assert (!p || (not !p && len = 0)) ;
        (* XXX(dinosaure): we gave a [o] buffer which is enough to store
           inflated data. At the end, [decoder] should not return more than one
           [`Flush]. A special case is when we inflate nothing: [`Flush] never
           appears and we reach [`End] directly, so [!p (still) = false and len (must) = 0]. *)
        return { kind; raw; len= !l; depth= 1; }
      | `Flush decoder ->
        l := Bigstringaf.length o - Zz.M.dst_rem decoder ;
        assert (not !p) ; p := true ;
        let decoder = Zz.M.flush decoder in
        (go[@tailcall]) cursor decoder
      | `Await decoder ->
        W.load s ~map t.ws cursor >>= function
        | Some slice ->
          let off = cursor - slice.W.offset in
          let len = slice.W.length - off in
          let decoder = Zz.M.src decoder slice.W.payload off len in
          (go[@tailcall]) (slice.W.offset + slice.W.length) decoder
        | None ->
          let decoder = Zz.M.src decoder Bigstringaf.empty 0 0 in
          (go[@tailcall]) cursor decoder in
    let off = cursor - slice.W.offset in
    let len = slice.W.length - off in
    let decoder = Zz.M.src decoder slice.W.payload off len in
    go (slice.W.offset + slice.W.length) decoder

let of_delta
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> kind -> raw -> depth:int -> cursor:int -> W.slice -> (v, s) io
  = fun ({ bind; return; } as s) ~map t kind raw ~depth ~cursor slice ->
    let ( >>= ) = bind in

    let decoder = Zh.M.decoder ~o:t.tmp ~allocate:t.allocate `Manual in

    let rec go cursor decoder = match Zh.M.decode decoder with
      | `End decoder ->
        let len = Zh.M.dst_len decoder in
        return { kind; raw; len; depth; }
      | `Malformed err -> failwith err
      | `Header (src_len, dst_len, decoder) ->
        let source = get_source raw in
        let payload = get_payload raw in

        assert (Bigstringaf.length source >= src_len) ;
        assert (Bigstringaf.length payload >= dst_len) ;

        let decoder = Zh.M.source decoder source in
        let decoder = Zh.M.dst decoder payload 0 dst_len in
        (go[@tailcall]) cursor decoder
      | `Await decoder ->
        W.load s ~map t.ws cursor >>= function
        | None ->
          let decoder = Zh.M.src decoder Bigstringaf.empty 0 0 in
          (go[@tailcall]) cursor decoder
        | Some slice ->
          let off = cursor - slice.W.offset in
          let len = slice.W.length - off in
          let decoder = Zh.M.src decoder slice.W.payload off len in
          (go[@tailcall]) (slice.W.offset + slice.W.length) decoder in
    let off = cursor - slice.W.offset in
    let len = slice.W.length - off in
    let decoder = Zh.M.src decoder slice.W.payload off len in
    go (slice.W.offset + slice.W.length) decoder

let rec of_ofs_delta
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> raw -> anchor:int -> cursor:int -> W.slice -> (v, s) io
  = fun ({ bind; _ } as s) ~map t raw ~anchor ~cursor slice ->
    let ( >>= ) = bind in

    header_of_ofs_delta s ~map t cursor slice >>= fun (base_offset, pos, slice) ->
    of_offset s ~map t (flip raw) ~cursor:(anchor - base_offset) >>= fun v ->
    of_delta s ~map t v.kind raw ~depth:(succ v.depth) ~cursor:(slice.W.offset + pos) slice

and of_ref_delta
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> raw -> cursor:int -> W.slice -> (v, s) io
  = fun ({ bind; _ } as s) ~map t raw ~cursor slice ->
  let ( >>= ) = bind in

  header_of_ref_delta s ~map t cursor slice >>= fun (uid, pos, slice) ->
  of_uid s ~map t (flip raw) uid >>= fun v ->
  of_delta s ~map t v.kind raw ~depth:(succ v.depth) ~cursor:(slice.W.offset + pos) slice

and of_uid
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> raw -> uid -> (v, s) io
  = fun s ~map t raw uid ->
    let cursor = t.fd uid in
    of_offset s ~map t raw ~cursor

and of_offset
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> raw -> cursor:int -> (v, s) io
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
  ; depth : int
  ; kind : [ `A | `B | `C | `D ] }

let path_to_list { path; depth; _ } =
  Array.sub path 0 depth |> Array.to_list

let kind_of_int = function
  | 0b001 -> `A
  | 0b010 -> `B
  | 0b011 -> `C
  | 0b100 -> `D
  | _ -> assert false

let rec fill_path_from_ofs_delta
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> depth:int -> int array -> anchor:int -> cursor:int -> W.slice -> (int * [ `A | `B | `C | `D ], s) io
  = fun ({ bind; _ } as s) ~map t ~depth path ~anchor ~cursor slice ->
    let ( >>= ) = bind in

    header_of_ofs_delta s ~map t cursor slice >>= fun (base_offset, _, _) ->
    (fill_path_from_offset[@tailcall]) s ~map t ~depth:(succ depth) path ~cursor:(anchor - base_offset)

and fill_path_from_ref_delta
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> depth:int -> int array -> cursor:int -> W.slice -> (int * [ `A | `B | `C | `D ], s) io
  = fun ({ bind; _ } as s) ~map t ~depth path ~cursor slice ->
  let ( >>= ) = bind in

  header_of_ref_delta s ~map t cursor slice >>= fun (uid, _, _) ->
  (fill_path_from_uid[@tailcall]) s ~map t ~depth path uid

and fill_path_from_uid
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> depth:int -> int array -> uid -> (int * [ `A | `B | `C | `D ], s) io
  = fun s ~map t ~depth path uid ->
    let cursor = t.fd uid in
    path.(depth - 1) <- cursor ;
    (fill_path_from_offset[@tailcall]) s ~map t ~depth:(succ depth) path ~cursor

and fill_path_from_offset
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> depth:int -> int array -> cursor:int -> (int * [ `A | `B | `C | `D ], s) io
  = fun ({ return; bind; } as s) ~map t ~depth path ~cursor ->
    let ( >>= ) = bind in
    W.load s ~map t.ws cursor >>= function
    | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_offset])" cursor
    | Some slice ->

      path.(depth - 1) <- cursor ;
      header_of_entry s ~map t cursor slice >>= fun (kind, _, pos, slice) ->

      match kind with
      | 0b000 | 0b101 -> failwith "bad type"
      | (0b001 | 0b010 | 0b011 | 0b100) as v ->
        return (depth, kind_of_int v)
      | 0b110 ->
        (fill_path_from_ofs_delta[@tailcall]) s ~map t ~depth path ~anchor:cursor ~cursor:(slice.W.offset + pos) slice
      | 0b111 ->
        (fill_path_from_ref_delta[@tailcall]) s ~map t ~depth path ~cursor:(slice.W.offset + pos) slice
      | _ -> assert false

let path_of_offset
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> cursor:int -> (path, s) io
  = fun ({ return; bind; } as s) ~map t ~cursor ->
    let ( >>= ) = bind in
    let path = Array.make _max_depth 0 in
    fill_path_from_offset s ~map t ~depth:1 path ~cursor >>= fun (depth, kind) ->
    return { depth; path; kind; }

let path_of_uid
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> uid -> (path, s) io
  = fun s ~map t uid ->
    let cursor = t.fd uid in
    path_of_offset s ~map t ~cursor

let of_offset_with_source
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> kind -> raw -> depth:int -> cursor:int -> (v, s) io
  = fun ({ bind; _ } as s) ~map t kind raw ~depth ~cursor ->
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
        header_of_ofs_delta s ~map t cursor slice >>= fun (_, pos, slice) ->
        of_delta s ~map t kind raw ~depth ~cursor:(slice.W.offset + pos) slice
      | 0b111 ->
        let cursor = slice.W.offset + pos in
        header_of_ref_delta s ~map t cursor slice >>= fun (_, pos, slice) ->
        of_delta s ~map t kind raw ~depth ~cursor:(slice.W.offset + pos) slice
      | _ -> assert false

let base_of_offset
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> raw -> cursor:int -> (v, s) io
  = fun ({ bind; _ } as s) ~map t raw ~cursor ->
    let ( >>= ) = bind in
    W.load s ~map t.ws cursor >>= function
    | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_offset])" cursor
    | Some slice ->
      header_of_entry s ~map t cursor slice >>= fun (hdr, _, pos, slice) ->
      let kind = match hdr with
        | 0b001 -> `A | 0b010 -> `B | 0b011 -> `C | 0b100 -> `D | _ -> failwith "Invalid object" in
      uncompress s ~map t kind raw ~cursor:(slice.W.offset + pos) slice

let base_of_path { depth; path; _ } = path.(depth - 1)
let kind_of_path { kind; _ } = kind

let of_offset_with_path
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> (fd, uid) t -> path:path -> raw -> cursor:int -> (v, s) io
  = fun ({ bind; return; } as s) ~map t ~path raw ~cursor ->
    assert (cursor == path.path.(0)) ;
    let ( >>= ) = bind in

    base_of_offset s ~map t raw ~cursor:(base_of_path path) >>= fun base ->
    let rec go depth raw =
      of_offset_with_source s ~map t base.kind raw ~depth ~cursor:path.path.(depth - 1) >>= fun v ->
      if depth == 1
      then return v
      else (go[@tailcall]) (pred depth) (flip raw) in
    if path.depth > 1 then go (path.depth - 1) (flip raw) else return base

type 'uid digest = kind:kind -> ?off:int -> ?len:int -> Bigstringaf.t -> 'uid

let uid_of_offset
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> digest:uid digest -> (fd, uid) t -> raw -> cursor:int -> (kind * uid, s) io
  = fun ({ bind; return; } as s) ~map ~digest t raw ~cursor ->
    let ( >>= ) = bind in
    W.load s ~map t.ws cursor >>= function
    | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_offset])" cursor
    | Some slice ->
      header_of_entry s ~map t cursor slice >>= fun (hdr, _, pos, slice) ->
      let kind = match hdr with
        | 0b001 -> `A | 0b010 -> `B | 0b011 -> `C | 0b100 -> `D | _ -> failwith "Invalid object" in
      uncompress s ~map t kind raw ~cursor:(slice.W.offset + pos) slice >>= fun v ->
      return (kind, digest ~kind ~len:v.len (get_payload raw))

let uid_of_offset_with_source
  : type fd uid s. s scheduler -> map:(fd, s) W.map -> digest:uid digest -> (fd, uid) t -> kind:kind -> raw -> depth:int -> cursor:int -> (uid, s) io
  = fun ({ bind; return; } as s) ~map ~digest t ~kind raw ~depth ~cursor ->
    let ( >>= ) = bind in
    W.load s ~map t.ws cursor >>= function
    | None -> Fmt.failwith "Reach end of pack (ask: %d, [weight_of_offset])" cursor
    | Some slice ->
      header_of_entry s ~map t cursor slice >>= fun (hdr, _, pos, slice) ->

      match hdr with
      | 0b000 | 0b101 -> failwith "bad type"
      | 0b001 ->
        assert (kind = `A) ;
        assert (depth = 1) ;
        uncompress s ~map t `A raw ~cursor:(slice.W.offset + pos) slice >>= fun v ->
        return (digest ~kind ~len:v.len (get_payload raw))
      | 0b010 ->
        assert (kind = `B) ;
        assert (depth = 1) ;
        uncompress s ~map t `B raw ~cursor:(slice.W.offset + pos) slice >>= fun v ->
        return (digest ~kind ~len:v.len (get_payload raw))
      | 0b011 ->
        assert (kind = `C) ;
        assert (depth = 1) ;
        uncompress s ~map t `C raw ~cursor:(slice.W.offset + pos) slice >>= fun v ->
        return (digest ~kind ~len:v.len (get_payload raw))
      | 0b100 ->
        assert (kind = `D) ;
        assert (depth = 1) ;
        uncompress s ~map t `D raw ~cursor:(slice.W.offset + pos) slice >>= fun v ->
        return (digest ~kind ~len:v.len (get_payload raw))
      | 0b110 ->
        header_of_ofs_delta s ~map t (slice.W.offset + pos) slice >>= fun (_, pos, slice) ->
        of_delta s ~map t kind raw ~depth ~cursor:(slice.W.offset + pos) slice >>= fun v ->
        return (digest ~kind ~len:v.len (get_payload raw))
      | 0b111 ->
        header_of_ref_delta s ~map t (slice.W.offset + pos) slice >>= fun (_, pos, slice) ->
        of_delta s ~map t kind raw ~depth ~cursor:(slice.W.offset + pos) slice >>= fun v ->
        return (digest ~kind ~len:v.len (get_payload raw))
      | _ -> assert false

type 'uid node =
  | Node of int * 'uid * 'uid node list
  | Leaf of int * 'uid
and 'uid tree = Base of kind * int * 'uid * 'uid node list

type 'uid children = cursor:int -> uid:'uid -> int list
type where = cursor:int -> int

type 'uid oracle =
  { digest : 'uid digest
  ; children : 'uid children
  ; where : where
  ; weight : cursor:int -> int } (* TODO: hide it with [weight]. *)

module Verify (Uid : UID) (Scheduler : SCHEDULER) (IO : IO with type 'a t = 'a Scheduler.s) = struct
  let s =
    let open Scheduler in
    { bind= (fun x f -> inj (IO.bind (prj x) (fun x -> prj (f x))))
    ; return= (fun x -> inj (IO.return x)) }

  let ( >>= ) = IO.bind

  type status =
    | Unresolved_base of int
    | Unresolved_node
    | Resolved_base of Uid.t * kind
    | Resolved_node of Uid.t * kind * int * Uid.t

  let uid_of_status = function
    | Resolved_node (uid, _, _, _) | Resolved_base (uid, _) -> uid
    | Unresolved_node ->
      Fmt.invalid_arg "Current status is not resolved"
    | Unresolved_base offset ->
      Fmt.invalid_arg "Current status is not resolved (offset: %d)" offset

  let kind_of_status = function
    | Resolved_base (_, kind) | Resolved_node (_, kind, _, _) -> kind
    | _ -> Fmt.invalid_arg "Current status is not resolved"

  let depth_of_status = function
    | Resolved_base _ | Unresolved_base _ -> 0
    | Resolved_node (_, _, depth, _) -> depth
    | Unresolved_node -> Fmt.invalid_arg "Current status is not resolved"

  let source_of_status = function
    | Resolved_base _ | Unresolved_base _ -> None
    | Resolved_node (_, _, _, source) -> Some source
    | Unresolved_node -> Fmt.invalid_arg "Current status is not resolved"

  let rec nodes_of_offsets
    : type fd. map:(fd, Scheduler.t) W.map -> oracle:Uid.t oracle -> (fd, Uid.t) t -> kind:kind -> raw -> depth:int -> cursors:int list -> Uid.t node list IO.t
    = fun ~map ~oracle t ~kind raw ~depth ~cursors ->
      match cursors with
      | [] -> invalid_arg "Expect at least one cursor"
      | [ cursor ] ->
        uid_of_offset_with_source s ~map ~digest:oracle.digest t ~kind raw ~depth ~cursor |> Scheduler.prj >>= fun uid ->
        ( match oracle.children ~cursor ~uid with
          | [] -> IO.return [ Leaf (cursor, uid) ]
          | cursors ->
            nodes_of_offsets ~map ~oracle t ~kind (flip raw) ~depth:(succ depth) ~cursors >>= fun nodes ->
            IO.return [ Node (cursor, uid, nodes) ] )
      | cursors ->
        let source = get_source raw in
        let source = Bigstringaf.copy ~off:0 ~len:(Bigstringaf.length source) source in (* allocation *)
        let res = Array.make (List.length cursors) (Leaf ((-1), Uid.null)) in

        let fibers =
          List.mapi (fun i cursor ->
              uid_of_offset_with_source s ~map ~digest:oracle.digest t ~kind raw ~depth ~cursor |> Scheduler.prj >>= fun uid ->
              match oracle.children ~cursor ~uid with
              | [] ->
                res.(i) <- Leaf (cursor, uid) ; IO.return ()
              | cursors ->
                nodes_of_offsets ~map ~oracle t ~kind (flip raw) ~depth:(succ depth) ~cursors >>= fun nodes ->
                Bigstringaf.blit source ~src_off:0 (get_source raw) ~dst_off:0 ~len:(Bigstringaf.length source) ;
                res.(i) <- Node (cursor, uid, nodes) ; IO.return () )
            cursors in
        IO.all_unit fibers >>= fun () ->
        IO.return (Array.to_list res)

  let weight_of_tree
    : oracle:Uid.t oracle -> cursor:int -> int
    = fun ~oracle ~cursor ->
      let rec go cursor w0 =
        let w1 = oracle.weight ~cursor in
        match oracle.children ~cursor ~uid:Uid.null with
        | [] -> (max : int -> int -> int) w0 w1
        | cursors ->
          let w1 = ref w1 in
          List.iter (fun cursor -> w1 := go cursor !w1) cursors ;
          (max : int -> int -> int) w0 !w1 in
      go cursor 0 (* XXX(dinosaure): we can do something which is tail-rec, TODO! *)

  let resolver
    : type fd. map:(fd, Scheduler.t) W.map -> oracle:Uid.t oracle -> (fd, Uid.t) t -> cursor:int -> Uid.t tree IO.t
    = fun ~map ~oracle t ~cursor ->
      let weight = weight_of_tree ~oracle ~cursor in
      let raw = make_raw ~weight in (* allocation *)
      uid_of_offset s ~map ~digest:oracle.digest t raw ~cursor |> Scheduler.prj >>= fun (kind, uid) ->
      match oracle.children ~cursor ~uid with
      | [] -> IO.return (Base (kind, cursor, uid, []))
      | cursors ->
        nodes_of_offsets ~map ~oracle t ~kind (flip raw) ~depth:1 ~cursors >>= fun nodes ->
        IO.return (Base (kind, cursor, uid, nodes))

  let update
    : type fd. map:(fd, Scheduler.t) W.map -> oracle:Uid.t oracle -> (fd, Uid.t) t -> cursor:int -> matrix:status array -> unit IO.t
    = fun ~map ~oracle t ~cursor ~matrix ->
      resolver ~map ~oracle t ~cursor >>= fun (Base (kind, cursor, uid, children)) ->
      matrix.(oracle.where ~cursor) <- Resolved_base (uid, kind) ;
      let rec go depth source = function
        | Leaf (cursor, uid) ->
          matrix.(oracle.where ~cursor) <- Resolved_node (uid, kind, depth, source)
        | Node (cursor, uid, children) ->
          matrix.(oracle.where ~cursor) <- Resolved_node (uid, kind, depth, source) ; List.iter (go (succ depth) uid) children in
      List.iter (go 1 uid) children ; IO.return ()

  type m = { mutable v : int; m : IO.Mutex.t }

  let is_not_unresolved_base = function
    | Unresolved_base _ -> false
    | _ -> true

  let unresolved_base ~cursor = Unresolved_base cursor
  let unresolved_node = Unresolved_node

  let dispatcher
    : type fd. map:(fd, Scheduler.t) W.map -> oracle:Uid.t oracle -> (fd, Uid.t) t -> matrix:status array -> mutex:m -> unit IO.t
    = fun ~map ~oracle t ~matrix ~mutex ->
      let rec go () =
        IO.Mutex.lock mutex.m >>= fun () ->
        while mutex.v < Array.length matrix && is_not_unresolved_base matrix.(mutex.v)
        do mutex.v <- mutex.v + 1 done ;
        if mutex.v >= Array.length matrix
        then ( IO.Mutex.unlock mutex.m ; IO.return () )
        else ( let root = mutex.v in mutex.v <- mutex.v + 1 ; IO.Mutex.unlock mutex.m ;
               let[@warning "-8"] Unresolved_base cursor = matrix.(root) in (* XXX(dinosaure): Oh god, save me! *)
               update ~map ~oracle t ~cursor ~matrix >>= fun () ->
               (go[@tailcall]) () ) in
      go ()

  let verify
    : type fd. threads:int -> map:(fd, Scheduler.t) W.map -> oracle:Uid.t oracle -> (fd, Uid.t) t -> matrix:status array -> unit IO.t
    = fun ~threads ~map ~oracle t ~matrix ->
      let mutex = { v= 0; m= IO.Mutex.create () } in
      let t0 = t in

      IO.nfork_map
        ~f:(fun t -> dispatcher ~map ~oracle t ~matrix ~mutex)
        (List.init threads (fun _ ->
             let z = Bigstringaf.copy t0.tmp ~off:0 ~len:(Bigstringaf.length t0.tmp) in
             let w = t0.allocate 15 in
             { t0 with ws= W.make t0.ws.W.fd; tmp= z; allocate= (fun _ -> w) }))
      >>= fun futures -> IO.all_unit (List.map IO.Future.wait futures)
end

module Ip (Scheduler : SCHEDULER) (IO : IO with type 'a t = 'a Scheduler.s) (Uid : UID) = struct
  type optint = Idx.optint

  let ( >>= ) = IO.bind
  let return = IO.return

  module K = struct type t = Uid.t let compare = Uid.compare end
  module V = struct type t = int * optint let compare (a, _) (b, _) = (compare : int -> int -> int) a b end
  module Q = Psq.Make(K)(V)

  let consumer ~f ~q ~finish ~signal ~mutex =
    let rec go () =
      IO.Mutex.lock mutex >>= fun () ->
      let rec wait () =
        if Q.is_empty q.contents && not !finish
        then IO.Condition.wait signal mutex >>= wait
        else return () in
      wait () >>= fun () -> match Q.pop q.contents with
      | Some ((uid, (offset, crc)), q') ->
        q := q' ; IO.Mutex.unlock mutex ; f ~uid ~offset ~crc >>= go
      | None -> assert (!finish) ; IO.Mutex.unlock mutex ; return () in
    go ()

  let producer ~idx ~q ~finish ~signal ~mutex =
    let p = ref 0 in

    let rec go () =
      IO.Mutex.lock mutex >>= fun () ->
      let v = !p in

      if v >= Idx.max idx
      then ( finish := true
           ; IO.Condition.broadcast signal
           ; IO.Mutex.unlock mutex
           ; return () )
      else
        ( incr p ;
          let uid = Idx.get_uid idx v
          and offset = Idx.get_offset idx v
          and crc = Idx.get_crc idx v in

          q := Q.add uid (offset, crc) !q ;

          IO.Condition.signal signal ; IO.Mutex.unlock mutex ; go () ) in
    go ()

  type 'a rdwr = Producer | Consumer of 'a

  (* XXX(dinosaure): priority queue is needed to avoid fragmentation of [mmap]
     and explosion of virtual memory. *)

  let iter ~threads ~f idx =
    let mutex = IO.Mutex.create () in
    let signal = IO.Condition.create () in
    let finish = ref false in
    let q = ref Q.empty in

    IO.nfork_map
      ~f:(function
          | Producer -> producer ~idx ~q ~finish ~signal ~mutex
          | Consumer t -> consumer ~f:(f t) ~q ~finish ~signal ~mutex)
      (Producer :: List.map (fun x -> Consumer x) threads)
    >>= fun futures -> IO.all_unit (List.map IO.Future.wait futures)
end
