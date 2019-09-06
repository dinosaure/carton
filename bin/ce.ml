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

let input_bigstring ic buf off len =
  let tmp = Bytes.create len in
  let res = input ic tmp 0 len in
  Bigstringaf.blit_from_bytes tmp ~src_off:0 buf ~dst_off:off ~len:res ; res

module type UID = sig
  type t
  type ctx

  val empty : ctx
  val get : ctx -> t
  val feed : ctx -> ?off:int -> ?len:int -> bigstring -> ctx

  val equal : t -> t -> bool
  val compare : t -> t -> int
  val length : int
  val of_raw_string : string -> t
  val pp : t Fmt.t
  val null : t
end

type kind = [ `A | `B | `C | `D ]

type 'uid entry =
  { uid : 'uid
  ; kind : kind
  ; length : int
  ; preferred : bool
  ; hash : int
  ; delta : 'uid delta }
and 'uid delta = From of 'uid | Zero

let kind_to_int = function
  | `A -> 0 | `B -> 1 | `C -> 2 | `D -> 3
let bool_to_int = function
  | false -> 0 | true -> 1

let compare_entry a b =
  if kind_to_int a.kind > kind_to_int b.kind then (-1)
  else if kind_to_int a.kind < kind_to_int b.kind then 1
  else if a.hash > b.hash then (-1)
  else if a.hash < b.hash then 1
  else if bool_to_int a.preferred > bool_to_int b.preferred then (-1)
  else if bool_to_int a.preferred < bool_to_int b.preferred then 1
  else if a.length > b.length then (-1)
  else if a.length < b.length then 1
  else compare a b

let hash x =
  let res = ref 0 in
  for i = 0 to String.length x - 1
  do if x.[i] <> ' ' then res := (!res lsr 2) + (Char.code x.[i] lsl 24) done ;
  !res

let make_entry ~kind ~length ?(preferred= false) ?(name= "") ?(delta= Zero) uid =
  { uid; kind; length; preferred; hash= hash name; delta; }

module Utils = struct
  let length_of_variable_length n =
    let rec go r = function 0 -> r | n -> go (succ r) (n lsr 7) in
    go 1 (n lsr 7)

  let length_of_copy_code n =
    let rec go r n = if n = 0 then r else go (succ r) (n lsr 8) in
    if n = 0 then 1 else go 0 n

  let length ~source ~target hunks =
    length_of_variable_length source +
    length_of_variable_length target +
    List.fold_left (fun acc -> function
        | Duff.Insert (_, len) -> 1 + len + acc
        | Duff.Copy (off, len) ->
          1 + length_of_copy_code off +
          (if len = 0x10000 then 1 else length_of_copy_code len) +
          acc)
      0 hunks

  let has_only_inserts = List.for_all (function Duff.Insert _ -> true | _ -> false)
end

module W = struct
  type 'uid p = { index : Duff.index
                ; entry : 'uid entry
                ; depth : int
                ; v : Cd.v }

  type 'uid q = { mutable patch : 'uid patch option
                ; entry : 'uid entry
                ; v : Cd.v }
  and 'uid patch = { hunks : Duff.hunk list
                   ; depth : int
                   ; source : 'uid
                   ; source_length : int }

  module K = struct type t = Uid.t let hash = Hashtbl.hash let equal = Uid.equal end
  module V = struct type t = Uid.t p let weight ({ v; _ } : Uid.t p) = Cd.len v end
  module Window = Lru.M.Make(K)(V)

  let uid_eq a b = (=) a b
  let uid_ln = 20
  let _max_depth = 60

  let length_of_delta ~source ~target hunks = Utils.length ~source ~target hunks
  let depth_of_source : 'uid p -> int = fun { depth; _ } -> depth
  let depth_of_target : 'uid q -> int = fun { patch; _ } -> match patch with
    | None -> 1 | Some { depth; _ } -> depth

  let same_island : 'uid -> 'uid -> bool = fun _ _ -> true
  (* XXX(dinosaure): TODO [delta-islands]! *)

  exception Break
  exception Next

  type ('uid, 's) load = 'uid -> (Cd.v, 's) io

  let entry_to_target
    : type uid s. s scheduler -> load:(uid, s) load -> uid entry -> (uid q, s) io
    = fun { bind; return; } ~load entry ->
      let ( >>= ) = bind in

      load entry.uid >>= fun v -> (match entry.delta with
          | From uid ->
            load uid >>= fun s ->
            let index = Duff.make (Bigstringaf.sub ~off:0 ~len:(Cd.len s) (Cd.raw s)) in
            let hunks = Duff.delta index (Bigstringaf.sub ~off:0 ~len:(Cd.len v) (Cd.raw v)) in
            return (Some { hunks; depth= Cd.depth v; source= uid; source_length= Cd.len s; })
          | Zero -> return None) >>= fun patch -> return { patch; entry; v; }

  let apply ~(source:Uid.t p) ~(target:Uid.t q) =
    if source.entry.kind <> target.entry.kind
    then raise_notrace Break ;

    if depth_of_source source >= _max_depth
    then raise_notrace Next ;

    let max_length, ref_depth = match target.patch with
      | Some { hunks; source_length; depth; _ } ->
        length_of_delta ~source:source_length ~target:target.entry.length hunks, depth
      | None ->
        target.entry.length / 2 - uid_ln, 1 in

    let max_length = max_length * (_max_depth - depth_of_source source) / (_max_depth - ref_depth + 1) in

    if max_length == 0 then raise_notrace Next ;

    let diff =
      if source.entry.length < target.entry.length
      then target.entry.length - source.entry.length else 0 in

    if diff >= max_length then raise_notrace Next ;
    if target.entry.length < source.entry.length / 32 then raise_notrace Next ;
    if not (same_island target.entry.uid source.entry.uid) then raise_notrace Next ;

    (* load index, source and target. *)

    let hunks = Duff.delta source.index (Bigstringaf.sub ~off:0 ~len:(Cd.len target.v) (Cd.raw target.v)) in
    target.patch <- Some { hunks
                         ; source= source.entry.uid
                         ; source_length= source.entry.length
                         ; depth= source.depth + 1; }

  let target_to_source (target : 'uid q) =
    { index= Duff.make (Bigstringaf.sub ~off:0 ~len:(Cd.len target.v) (Cd.raw target.v))
    ; entry= target.entry
    ; depth= depth_of_target target
    ; v= target.v }

  let delta targets =
    let window = Window.create 0x1000 in

    let go target =
      let best : Window.k option ref = ref None in
      let f (key : Window.k) source =
        try apply ~source ~target ; best := (Some key)
        with Next -> ()
           | Break as exn -> raise_notrace exn in
      ( try Window.iter f window
        with Break -> () ) ;
      ( match !best with
        | Some best ->
          let[@warning "-8"] Some source = Window.find best window in
          if source.depth < _max_depth
          then ( Window.add target.entry.uid (target_to_source target) window
               ; Window.promote best window )
        | None -> () ) in
    Array.iter go targets
end
