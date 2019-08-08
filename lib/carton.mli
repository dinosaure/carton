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

module Make (T : FUNCTOR) : SCHEDULER with type 'a s = 'a T.t

type bigstring = Bigstringaf.t

module W : sig
  type 'fd t
  and slice =
    { offset : int
    ; length : int
    ; payload : bigstring }
  and ('fd, 's) map = 'fd -> pos:int -> int -> (bigstring, 's) io
end

module Uid : sig
  type t = Digestif.SHA1.t

  val length : int
  val of_raw_string : string -> t
  val pp : t Fmt.t
end

type 'fd t

type raw =
  { raw0 : bigstring
  ; raw1 : bigstring
  ; flip : bool }

type v =
  { kind : [ `A | `B | `C | `D ]
  ; raw  : raw
  ; len  : int }

val make_raw : weight:int -> raw

val make : 'fd -> z:Zz.bigstring -> allocate:(int -> Zz.window) -> (Uid.t -> int) -> 'fd t
val weight_of_offset : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> int -> (int, 's) io
val weight_of_uid : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> Uid.t -> (int, 's) io
val of_offset : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> raw -> int -> (v, 's) io
