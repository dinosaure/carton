[@@@warning "-32"]

let input_bigstring ic buf off len =
  let tmp = Bytes.create len in
  let res = input ic tmp 0 len in
  Bigstringaf.blit_from_bytes tmp ~src_off:0 buf ~dst_off:off ~len:res ; res

module Uid = struct
  type t = Digestif.SHA1.t
  type ctx = Digestif.SHA1.ctx

  let empty = Digestif.SHA1.empty
  let get = Digestif.SHA1.get
  let feed ctx ?off ?len buf = Digestif.SHA1.feed_bigstring ctx ?off ?len buf
  let equal = Digestif.SHA1.equal
  let length = Digestif.SHA1.digest_size
  let of_raw_string : string -> t = Digestif.SHA1.of_raw_string
  let pp = Digestif.SHA1.pp
end

type src = [ `Channel of in_channel | `String of string | `Manual ]
type bigstring = Bigstringaf.t

type decoder =
  { src : src
  ; i : bigstring
  ; i_pos : int
  ; i_len : int
  ; n : int (* number of objects *)
  ; c : int (* counter of objects *)
  ; v : int (* version of PACK file *)
  ; r : int (* how many bytes consumed *)
  ; s : s
  ; o : bigstring
  ; t_tmp : bigstring
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
  | `End
  | `Malformed of string ]
and kind =
  | Base of [ `A | `B | `C | `D ]
  | Ofs of { sub : int; source : int; target : int; }
  | Ref of { ptr : Uid.t; source : int; target : int; }
and entry =
  { offset : int; kind : kind; size : int; }

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
    let k d =
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
        let e = { offset= d.r; kind= Base k; size= !size; } in

        decode { d with i_pos= !p; r= d.r + (!p - d.i_pos); c= succ d.c; z
                      ; s= Inflate e; k= decode
                      ; ctx= Uid.feed d.ctx d.i ~off:d.i_pos ~len:(!p - d.i_pos) }
      | 0b110 ->
        let offset = d.r in

        let k d =
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
          let e = { offset; kind= Ofs { sub= !base_offset; source= (-1); target= (-1); }; size= !size; } in

          decode { d with i_pos= !p; r= d.r + (!p - d.i_pos); c= succ d.c; z
                        ; s= Inflate e; k= decode
                        ; ctx= Uid.feed d.ctx d.i ~off:d.i_pos ~len:(!p - d.i_pos) } in

        peek_10 k { d with i_pos= !p; r= d.r + (!p - d.i_pos)
                         ; ctx= Uid.feed d.ctx d.i ~off:d.i_pos ~len:(!p - d.i_pos) }
      | _ -> assert false in
    peek_10 k d
  | Inflate ({ kind= Base _; _ } as entry) ->
    let rec go z = match Zz.M.decode z with
      | `Await z ->
        let len = i_rem d - Zz.M.src_rem z in
        refill decode { d with z; i_pos= d.i_pos + len; r= d.r + len
                             ; ctx= Uid.feed d.ctx d.i ~off:d.i_pos ~len }
      | `Flush z ->
        go (Zz.M.flush z)
      | `Malformed _ as err -> err
      | `End z ->
        let len = i_rem d - Zz.M.src_rem z in
        let z = Zz.M.reset z in
        let decoder = { d with i_pos= d.i_pos + len; r= d.r + len
                             ; z; s= if d.c == d.n then Hash else Entry
                             ; k= decode
                             ; ctx= Uid.feed d.ctx d.i ~off:d.i_pos ~len } in
        `Entry (entry, decoder) in
    go d.z
  | Inflate ({ kind= (Ofs _ | Ref _); _ } as entry) ->
    let source = ref (source entry) in
    let target = ref (target entry) in
    let first = ref (!source = (-1) && !target = (-1)) in

    let rec go z = match Zz.M.decode z with
      | `Await z ->
        let len = i_rem d - Zz.M.src_rem z in
        let entry = with_source !source entry in
        let entry = with_target !target entry in
        refill decode { d with z; i_pos= d.i_pos + len; r= d.r + len
                             ; s= Inflate entry
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
        let z = Zz.M.reset z in
        let decoder = { d with i_pos= d.i_pos + len; r= d.r + len
                             ; z; s= if d.c == d.n then Hash else Entry
                             ; k= decode
                             ; ctx= Uid.feed d.ctx d.i ~off:d.i_pos ~len } in
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
      then `End
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
