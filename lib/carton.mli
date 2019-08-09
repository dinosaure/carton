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

type weight [@@immediate]

val null : weight

type raw

val make_raw : weight:weight -> raw

type v

val kind : v -> [ `A | `B | `C | `D ]
val raw : v -> bigstring
val len : v -> int

val make : 'fd -> z:Zz.bigstring -> allocate:(int -> Zz.window) -> (Uid.t -> int) -> 'fd t

val weight_of_offset : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> weight:weight -> cursor:int -> (weight, 's) io
val weight_of_uid : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> weight:weight -> Uid.t -> (weight, 's) io

val of_offset : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> raw -> cursor:int -> (v, 's) io
val of_uid : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> raw -> Uid.t -> (v, 's) io

type path

val pp_path : path Fmt.t

val path_of_offset : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> cursor:int -> (path, 's) io
val of_offset_with_path : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> path:path -> raw -> cursor:int -> (v, 's) io
