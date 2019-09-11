module type MEMORY = sig
  type t
  type 'a fiber

  val map : t -> pos:int64 -> int -> Bigstringaf.t fiber
  val disconnect : t -> unit fiber
end

module type OBJECT = sig
  type uid
  type t
  type error = Mirage_kv.error
  type 'a fiber

  val pp_error : error Fmt.t
  val digest : Mirage_kv.Key.t -> (uid, error) result fiber
  val v : kind:[ `A | `B | `C | `D ] -> Bigstringaf.t -> off:int -> len:int -> (t, error) result
  val is : [ `A | `B | `C | `D ] -> [ `Dictionary | `Value ]
  val iter : t -> ((string * [ `Value | `Dictionary ]) list, error) result
end

module Make
    (Uid : Carton.UID)
    (Scheduler : Carton.SCHEDULER)
    (IO : Carton.IO with type 'a t = 'a Scheduler.s)
    (Object : OBJECT with type uid = Uid.t and type 'a fiber = 'a IO.t)
    (Memory : MEMORY with type 'a fiber = 'a IO.t)
  : Mirage_kv.RO with type value = Object.t
                  and type +'a io = 'a IO.t
                  and type error = private [> Mirage_kv.error ]
