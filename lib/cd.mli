(** {2 Prelude.}

    This module implements all {i read} operations on a [Carton] file. *)

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

module type MUTEX = sig
  type 'a fiber
  type t

  val create : unit -> t
  val lock : t -> unit fiber
  val unlock : t -> unit
end

module type FUTURE = sig
  type 'a fiber
  type 'a t

  val wait : 'a t -> 'a fiber
  val peek : 'a t -> 'a option
end

module type IO = sig
  type 'a t

  module Future : FUTURE with type 'a fiber = 'a t
  module Mutex : MUTEX with type 'a fiber = 'a t

  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val return : 'a -> 'a t
  val nfork_map : 'a list -> f:('a -> 'b t) -> 'b Future.t list t
  val all_unit : unit t list -> unit t
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

module type UID = sig
  type t
  type ctx

  val empty : ctx
  val get : ctx -> t
  val feed : ctx -> ?off:int -> ?len:int -> bigstring -> ctx

  val equal : t -> t -> bool
  val length : int
  val of_raw_string : string -> t
  val pp : t Fmt.t
  val null : t
end

type weight = private int

val null : weight
val weight_of_int_exn : int -> weight

type ('fd, 's) read = 'fd -> bytes -> off:int -> len:int -> (int, 's) io
type version

module Idx = Idx

module Fpass (Uid : UID) : sig
  type optint = Optint.t

  type kind =
    | Base of [ `A | `B | `C | `D ]
    | Ofs of { sub : int; source : weight; target : weight; }
    | Ref of { ptr : Uid.t; source : int; target : int; }

  type entry =
    { offset : int
    ; kind : kind
    ; size : weight
    ; consumed : int
    ; crc : optint }

  val check_header : 's scheduler -> ('fd, 's) read -> 'fd -> (version * int, 's) io

  type decoder

  type src =
    [ `Channel of in_channel | `String of string | `Manual ]
  type decode =
    [ `Await of decoder
    | `Peek of decoder
    | `Entry of (entry * decoder)
    | `End of Uid.t
    | `Malformed of string ]

  val decoder : o:Bigstringaf.t -> allocate:(int -> Dd.window) -> src -> decoder
  val decode : decoder -> decode

  val number : decoder -> int
  val version : decoder -> int
  val count : decoder -> int
end

type ('fd, 'uid) t
(** Type of state used to access to any objects into a [Carton] file. *)

type raw
(** Type of a [Carton] object as is into a [Carton] file. *)

val make_raw : weight:weight -> raw

type v
type kind = [ `A | `B | `C | `D ]

val kind : v -> kind
val raw : v -> bigstring
val len : v -> int

val make : 'fd -> z:Zz.bigstring -> allocate:(int -> Zz.window) -> uid_ln:int -> uid_rw:(string -> 'uid) -> ('uid -> int) -> ('fd, 'uid) t
(** [make fd ~z ~allocate ~uid_ln ~uid_rw where] returns a state associated to
   [fd] which is the user-defined representation of a [Carton] file. Some
   informations are needed:

   {ul
    {- [z] is an underlying buffer used to {i inflate} an object.}
    {- [allocate] is an {i allocator} of underlying {i window} used to {i
   inflate} an object.}
    {- [uid_ln] is the length of {i raw} representation of user-defined {i
   uid}.}
    {- [uid_rw] is the {i cast-function} from a string to user-defined {i uid}.}
    {- [where] is the function to associate an {i uid} to an {i offset} into the
   associated [Carton] file.} }

   Each argument depends on what the user wants. For example, if [t] is used by
   {!Verify.verify}, [allocate] {b must} be thread-safe according to {!IO}.
   [where] is not used by {!Verify.verify}. [uid_ln] and [uid_rw] depends on the
   [Carton] file associated by [fd]. Each functions available below describes
   precisely what they do on [t]. *)

(** {3 Weight of object.} *)

val weight_of_offset : 's scheduler -> map:('fd, 's) W.map -> ('fd, 'uid) t -> weight:weight -> cursor:int -> (weight, 's) io
val weight_of_uid : 's scheduler -> map:('fd, 's) W.map -> ('fd, 'uid) t -> weight:weight -> 'uid -> (weight, 's) io

(** {3 Value of object.} *)

val of_offset : 's scheduler -> map:('fd, 's) W.map -> ('fd, 'uid) t -> raw -> cursor:int -> (v, 's) io
val of_uid : 's scheduler -> map:('fd, 's) W.map -> ('fd, 'uid) t -> raw -> 'uid -> (v, 's) io

(** {3 Path of object.} *)

type path

val path_to_list : path -> int list

val path_of_offset : 's scheduler -> map:('fd, 's) W.map -> ('fd, 'uid) t -> cursor:int -> (path, 's) io
val path_of_uid : 's scheduler -> map:('fd, 's) W.map -> ('fd, 'uid) t -> 'uid -> (path, 's) io
val of_offset_with_path : 's scheduler -> map:('fd, 's) W.map -> ('fd, 'uid) t -> path:path -> raw -> cursor:int -> (v, 's) io

(** {3 Uid of object.} *)

type 'uid digest = kind:kind -> ?off:int -> ?len:int -> bigstring -> 'uid

val uid_of_offset : 's scheduler -> map:('fd, 's) W.map -> digest:'uid digest -> ('fd, 'uid) t -> raw -> cursor:int -> (kind * 'uid, 's) io
val uid_of_offset_with_source : 's scheduler -> map:('fd, 's) W.map -> digest:'uid digest -> ('fd, 'uid) t -> kind:kind -> raw -> cursor:int -> ('uid, 's) io

type 'uid children = cursor:int -> uid:'uid -> int list
type where = cursor:int -> int

type 'uid oracle =
  { digest : 'uid digest
  ; children : 'uid children
  ; where : where
  ; weight : cursor:int -> weight }

module Verify (Uid : UID) (Scheduler : SCHEDULER) (IO : IO with type 'a t = 'a Scheduler.s) : sig
  val s : Scheduler.t scheduler

  type status

  val uid_of_status : status -> Uid.t
  val kind_of_status : status -> kind
  val depth_of_status : status -> int
  val source_of_status : status -> Uid.t option

  val unresolved_base : cursor:int -> status
  val unresolved_node : status

  val verify : map:('fd, Scheduler.t) W.map -> oracle:Uid.t oracle -> ('fd, Uid.t) t -> matrix:status array -> unit IO.t
end
