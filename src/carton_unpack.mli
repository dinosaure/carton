module S = Carton_sigs

module Window :
sig
  type t

  val inside: int64 -> t -> bool
  val pp: t Fmt.t
end

module type H =
sig
  module Hash: S.HASH

  type error =
    | Reserved_opcode of int
    | Wrong_copy_hunk of int * int * int

  val pp_error : error Fmt.t

  type t =
    { i_off          : int
    ; i_pos          : int
    ; i_len          : int
    ; read           : int
    ; _length        : int
    ; _reference     : reference
    ; _source_length : int
    ; _target_length : int
    ; _hunk          : hunk option
    ; _tmp           : Cstruct.t
    ; state          : state
    ; }
  and k = Cstruct.t -> t -> res
  and state =
    | Header    of k
    | Stop
    | List      of k
    | Is_insert of (Cstruct.t * int * int)
    | Is_copy   of k
    | End
    | Exception of error
  and res =
    | Wait   of t
    | Error  of t * error
    | Cont   of t
    | Ok     of t * hunks
  and hunk =
    | Insert of Cstruct.t
    | Copy   of int * int
  and reference =
    | Offset of int64
    | Hash   of Hash.t
  and hunks =
    { reference     : reference
    ; length        : int
    ; source_length : int
    ; target_length : int
    ; }

  val partial_hunks: t -> hunks

  val pp_reference: reference Fmt.t
  val pp_hunks: hunks Fmt.t
  val pp: t Fmt.t

  val eval:
    Cstruct.t ->
    t ->
    [ `Await of t
    | `Error of t * error
    | `Hunk  of t * hunk
    | `Ok    of t * hunks ]

  val default: int -> reference -> t

  val refill: int -> int -> t -> t
  val continue: t -> t

  val current: t -> hunk
  val used_in: t -> int
  val available_in: t -> int
  val read: t -> int
end

module Hunk (Hash : S.HASH): H with module Hash = Hash

module type P =
sig
  module A: S.OBJECT
  module B: S.OBJECT
  module C: S.OBJECT
  module D: S.OBJECT

  module Hash: S.HASH
  module Checksum: S.CHECKSUM
  module Inflate: S.INFLATE
  module Hunk: H with module Hash := Hash

  type error =
    | Invalid_byte   of int
    | Reserved_kind  of int
    | Invalid_kind   of int
    | Inflate_error  of Inflate.error
    | Hunk_error     of Hunk.error
    | Invalid_length of int * int

  val pp_error : error Fmt.t

  type t

  type kind =
    | A
    | B
    | C
    | D
    | Hunk of Hunk.hunks

  val pp : t Fmt.t

  val default: Cstruct.t -> Inflate.window -> t
  val from_window: Window.t -> int -> Cstruct.t -> Inflate.window -> t
  val process_length: Window.t -> int -> Cstruct.t -> Inflate.window -> t
  val process_metadata: Window.t -> int -> Cstruct.t -> Inflate.window -> t

  val refill: int -> int -> t -> t
  val flush: int -> int -> t -> t
  val next_object: t -> t
  val continue: t -> t

  val many: t -> int32
  val kind: t -> kind
  val length: t -> int
  val offset: t -> int64
  val consumed: t -> int
  val checksum: t -> Checksum.t
  val output: t -> Cstruct.t * int

  val eval:
    Cstruct.t ->
    t ->
    [ `Await  of t
    | `End    of t * Hash.t
    | `Error  of t * error
    | `Flush  of t
    | `Hunk   of t * Hunk.hunk
    | `Object of t ]

  val eval_length:
    Cstruct.t ->
    t ->
    [ `Await  of t
    | `End    of t * Hash.t
    | `Error  of t * error
    | `Flush  of t
    | `Length of t ]

  val eval_metadata:
    Cstruct.t ->
    t ->
    [ `Await    of t
    | `End      of t * Hash.t
    | `Error    of t * error
    | `Flush    of t
    | `Metadata of t ]
end

module Pack
    (A : S.OBJECT)
    (B : S.OBJECT)
    (C : S.OBJECT)
    (D : S.OBJECT)
    (Hash : S.HASH)
    (Checksum : S.CHECKSUM)
    (Inflate : S.INFLATE)
    (Hunk : H with module Hash := Hash)
  : P with module A = A
       and module B = B
       and module C = C
       and module D = D
       and module Hash = Hash
       and module Checksum = Checksum
       and module Inflate = Inflate
       and module Hunk := Hunk

module type D =
sig
  module A: S.OBJECT
  module B: S.OBJECT
  module C: S.OBJECT
  module D: S.OBJECT

  module Hash: S.HASH
  module Checksum: S.CHECKSUM
  module Monad: S.MONAD
  module Mapper: S.MAPPER with type +'a m = 'a Monad.t
  module Inflate: S.INFLATE
  module Hunk: H with module Hash := Hash
  module Pack: P
    with module A := A
     and module B := B
     and module C := C
     and module D := D
     and module Hash := Hash
     and module Checksum := Checksum
     and module Inflate := Inflate
     and module Hunk := Hunk

  type error =
    | Invalid_hash of Hash.t
    | Invalid_offset of int64
    | Invalid_target of (int * int)
    | Unpack_error of Pack.t * Window.t * Pack.error
    | Mapper_error of Mapper.error

  val pp_error : error Fmt.t

  type t

  type kind = [ `A | `B | `C | `D ]

  module Object:
  sig
    type from =
      | Offset of { length: int
                  ; consumed: int
                  ; offset: int64
                  ; checksum: Checksum.t
                  ; base: from
                  ; }
      | External of Hash.t
      | Direct of { consumed : int
                  ; offset : int64
                  ; checksum : Checksum.t
                  ; }
    and t =
      { kind: kind
      ; raw: Cstruct.t
      ; length: int64
      ; from: from
      ; }

    val pp: t Fmt.t

    val top_checksum_exn: t -> Checksum.t
  end

  val find_window: t -> int64 -> ((Window.t * int), Mapper.error) result Monad.t

  val make:
    ?bucket:int ->
    Mapper.location ->
    (Hash.t -> Object.t option) ->
    (Hash.t -> (Checksum.t * int64) option) ->
    (int64 -> Hash.t option) ->
    (Hash.t -> (kind * Cstruct.t) option Monad.t) ->
    (t, Mapper.error) result Monad.t

  val idx: t -> Hash.t -> (Checksum.t * int64) option
  val cache: t -> Hash.t -> Object.t option
  val revidx: t -> int64 -> Hash.t option
  val extern: t -> Hash.t -> (kind * Cstruct.t) option Monad.t

  val update_idx: (Hash.t -> (Checksum.t * int64) option) -> t -> t
  val update_cache: (Hash.t -> Object.t option) -> t -> t
  val update_revidx: (int64 -> Hash.t option) -> t -> t
  val update_extern: (Hash.t -> (kind * Cstruct.t) option Monad.t) -> t -> t

  val length :
    ?chunk:int ->
    t ->
    Hash.t -> Cstruct.t -> Inflate.window -> (int, error) result Monad.t

  val needed_from_hash :
    ?chunk:int ->
    ?cache:(Hash.t -> int option) ->
    t ->
    Hash.t -> Cstruct.t -> Inflate.window -> (int, error) result Monad.t

  val get_from_offset :
    ?chunk:int ->
    ?limit:bool ->
    ?htmp:Cstruct.t array ->
    t ->
    int64 ->
    Cstruct.t * Cstruct.t * int ->
    Cstruct.t -> Inflate.window -> (Object.t, error) result Monad.t

  val get_from_hash :
    ?chunk:int ->
    ?limit:bool ->
    ?htmp:Cstruct.t array ->
    t ->
    Hash.t ->
    Cstruct.t * Cstruct.t * int ->
    Cstruct.t -> Inflate.window -> (Object.t, error) result Monad.t

  val get_with_hunks_allocation_from_offset :
    ?chunk:int ->
    t ->
    int64 ->
    Cstruct.t ->
    Inflate.window ->
    Cstruct.t * Cstruct.t -> (Object.t, error) result Monad.t

  val get_with_hunks_allocation_from_hash :
    ?chunk:int ->
    t ->
    Hash.t ->
    Cstruct.t ->
    Inflate.window ->
    Cstruct.t * Cstruct.t -> (Object.t, error) result Monad.t

  val get_with_result_allocation_from_hash :
    ?chunk:int ->
    ?htmp:Cstruct.t array ->
    t ->
    Hash.t ->
    Cstruct.t -> Inflate.window -> (Object.t, error) result Monad.t

  val get_with_result_allocation_from_offset :
    ?chunk:int ->
    ?htmp:Cstruct.t array ->
    t ->
    int64 ->
    Cstruct.t -> Inflate.window -> (Object.t, error) result Monad.t
end

module Decoder
    (A: S.OBJECT)
    (B: S.OBJECT)
    (C: S.OBJECT)
    (D: S.OBJECT)
    (Hash: S.HASH)
    (Checksum: S.CHECKSUM)
    (Monad: S.MONAD)
    (Mapper: S.MAPPER with type +'a m = 'a Monad.t)
    (Inflate: S.INFLATE)
    (Hunk: H with module Hash := Hash)
    (Pack: P with module A := A
                     and module B := B
                     and module C := C
                     and module D := D
                     and module Hash := Hash
                     and module Checksum := Checksum
                     and module Inflate := Inflate
                     and module Hunk := Hunk)
  : D with module A = A
       and module B = B
       and module C = C
       and module D = D
       and module Hash = Hash
       and module Checksum = Checksum
       and module Monad = Monad
       and module Mapper = Mapper
       and module Inflate = Inflate
       and module Hunk := Hunk
       and module Pack := Pack

module Stream
    (A: S.OBJECT)
    (B: S.OBJECT)
    (C: S.OBJECT)
    (D: S.OBJECT)
    (Hash: S.HASH)
    (Checksum: S.CHECKSUM)
    (Inflate: S.INFLATE) : sig
  include P
end with module A = A
     and module B = B
     and module C = C
     and module D = D
     and module Hash = Hash
     and module Inflate = Inflate
     and module Checksum = Checksum

module Random_access
    (A: S.OBJECT)
    (B: S.OBJECT)
    (C: S.OBJECT)
    (D: S.OBJECT)
    (Hash: S.HASH)
    (Checksum: S.CHECKSUM)
    (Monad: S.MONAD)
    (Mapper: S.MAPPER with type +'a m = 'a Monad.t)
    (Inflate: S.INFLATE) : sig
  include D
end with module A = A
     and module B = B
     and module C = C
     and module D = D
     and module Hash = Hash
     and module Checksum = Checksum
     and module Monad = Monad
     and module Mapper = Mapper
     and module Inflate = Inflate
