open Sigs

type ('uid, 'v) entry
type 'uid delta = From of 'uid | Zero

val make_entry : kind:kind -> length:int -> ?preferred:bool -> ?delta:'uid delta -> 'uid -> 'v -> ('uid, 'v) entry

type ('uid, 'v) q and ('uid, 'v) p and 'uid patch

type ('uid, 's) load = 'uid -> (Dec.v, 's) io
type ('uid, 's) find = 'uid -> (int option, 's) io

type 'uid uid =
  { uid_ln : int
  ; uid_rw : 'uid -> string }

val target_to_source : ('uid, 'v) q -> ('uid, 'v) p
val target_uid : ('uid, 'v) q -> 'uid

val value : ('uid, 'v) entry -> 'v
val entry_to_target : 's scheduler -> load:('uid, 's) load -> ('uid, 'v) entry -> (('uid, 'v) q, 's) io
val apply : 's scheduler -> load:('uid, 's) load -> uid_ln:int -> source:('uid, 'v) p -> target:('uid, 'v) q -> (unit, 's) io

module type VERBOSE = sig
  type 'a fiber

  val succ : unit -> unit fiber
  val print : unit -> unit fiber
end

module type UID = sig type t val hash : t -> int val equal : t -> t -> bool end

module Delta
    (Scheduler : SCHEDULER)
    (IO : IO with type 'a t = 'a Scheduler.s)
    (Uid : UID)
    (Verbose : VERBOSE with type 'a fiber = 'a IO.t) : sig
  val s : Scheduler.t scheduler

  val delta :
    threads:(Uid.t, Scheduler.t) load list ->
    weight:int ->
    uid_ln:int ->
    (Uid.t, 'v) entry array -> (Uid.t, 'v) q array IO.t
end

module N : sig
  type encoder

  type b =
    { i : Bigstringaf.t
    ; q : De.Queue.t
    ; w : De.window }

  val encoder : 's scheduler -> b:b -> load:('uid, 's) load -> ('uid, 'v) q -> (encoder, 's) io
  val encode : o:Bigstringaf.t -> encoder -> [ `Flush of (encoder * int) | `End ]
  val dst : encoder -> Bigstringaf.t -> int -> int -> encoder
end

type b =
  { i : Bigstringaf.t
  ; q : De.Queue.t
  ; w : De.window
  ; o : Bigstringaf.t }

val header_of_pack : length:int -> Bigstringaf.t -> int -> int -> unit
val encode_target : 's scheduler -> b:b -> find:('uid, 's) find -> load:('uid, 's) load -> uid:'uid uid -> ('uid, 'v) q -> cursor:int -> (int * N.encoder, 's) io
