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

module Fpass = Fpass

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

type digest = kind:[ `A | `B | `C | `D ] -> ?off:int -> ?len:int -> bigstring -> Uid.t

val uid_of_offset : 's scheduler -> map:('fd, 's) W.map -> digest:digest -> 'fd t -> raw -> cursor:int -> ([ `A | `B | `C | `D ] * Uid.t, 's) io
val uid_of_offset_with_source : 's scheduler -> map:('fd, 's) W.map -> digest:digest -> 'fd t -> kind:[ `A | `B | `C | `D ] -> raw -> cursor:int -> (Uid.t, 's) io

type children = cursor:int -> uid:Uid.t -> int list
type where = cursor:int -> int

type oracle =
  { digest : digest
  ; children : children
  ; where : where
  ; weight : cursor:int -> int }

module type IO = sig
  type 'a t

  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val return : 'a -> 'a t

  val list_iteri : (int -> 'a -> unit t) -> 'a list -> unit t

  type mutex

  val mutex : unit -> mutex
  val mutex_lock : mutex -> unit t
  val mutex_unlock : mutex -> unit

  type 'a u

  val task : unit -> 'a t * 'a u
  val async : (unit -> 'a t) -> unit
  val join : unit t list -> unit t
  val wakeup : 'a u -> 'a -> unit
end

module Verify (IO : IO) : sig
  module Scheduler : module type of Make(IO)

  val s : Scheduler.t scheduler

  type status

  val uid_of_status : status -> Uid.t

  val unresolved_base : cursor:int -> status
  val unresolved_node : status

  val verify : map:('fd, Scheduler.t) W.map -> oracle:oracle -> 'fd t -> matrix:status array -> unit IO.t
end
