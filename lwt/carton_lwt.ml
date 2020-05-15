open Lwt_io

let lwt_bind x f =
  let open Lwt.Infix in
  inj (prj x >>= fun x -> prj (f x))
[@@inline]

let lwt_return x = inj (Lwt.return x) [@@inline]

let lwt =
  { Carton.bind= lwt_bind
  ; Carton.return= lwt_return }

module Dec = struct
  module W = struct
    type 'fd t = 'fd Carton.Dec.W.t

    and slice = Carton.Dec.W.slice =
      { offset : int64
      ; length : int
      ; payload : Bigstringaf.t }
    and 'fd map =
      'fd -> pos:int64 -> int -> Bigstringaf.t Lwt.t

    let make fd = Carton.Dec.W.make fd
  end

  type weight = Carton.Dec.weight

  type 'fd read = 'fd -> bytes -> off:int -> len:int -> int Lwt.t

  module Idx = Carton.Dec.Idx

  module Fp (Uid : Carton.UID) = struct
    include Carton.Dec.Fp (Uid)

    let check_header read fd =
      let read fd buf ~off ~len =
        inj (read fd buf ~off ~len) in
      prj (check_header lwt read fd)
  end

  type ('fd, 'uid) t = ('fd, 'uid) Carton.Dec.t

  let with_z buf t = Carton.Dec.with_z buf t
  let with_w lru t = Carton.Dec.with_w lru t
  let with_allocate ~allocate t = Carton.Dec.with_allocate ~allocate t
  let fd t = Carton.Dec.fd t

  type raw = Carton.Dec.raw

  let make_raw ~weight = Carton.Dec.make_raw ~weight

  type v = Carton.Dec.v

  let v ~kind ?depth buf = Carton.Dec.v ~kind ?depth buf
  let kind v = Carton.Dec.kind v
  let raw v = Carton.Dec.raw v
  let len v = Carton.Dec.len v
  let depth v = Carton.Dec.depth v

  let make fd ~z ~allocate ~uid_ln ~uid_rw where =
    Carton.Dec.make fd ~z ~allocate ~uid_ln ~uid_rw where

  let weight_of_offset ~map t ~weight ~cursor =
    let map fd ~pos len =
      inj (map fd ~pos len) in
    prj (Carton.Dec.weight_of_offset lwt ~map t ~weight ~cursor)

  let weight_of_uid ~map t ~weight uid =
    let map fd ~pos len =
      inj (map fd ~pos len) in
    prj (Carton.Dec.weight_of_uid lwt ~map t ~weight uid)

  let of_offset ~map t raw ~cursor =
    let map fd ~pos len =
      inj (map fd ~pos len) in
    prj (Carton.Dec.of_offset lwt ~map t raw ~cursor)

  let of_uid ~map t raw uid =
    let map fd ~pos len =
      inj (map fd ~pos len) in
    prj (Carton.Dec.of_uid lwt ~map t raw uid)

  type path = Carton.Dec.path

  let path_to_list path = Carton.Dec.path_to_list path
  let kind_of_path path = Carton.Dec.kind_of_path path

  let path_of_offset ~map t ~cursor =
    let map fd ~pos len =
      inj (map fd ~pos len) in
    prj (Carton.Dec.path_of_offset lwt ~map t ~cursor)

  let path_of_uid ~map t uid =
    let map fd ~pos len =
      inj (map fd ~pos len) in
    prj (Carton.Dec.path_of_uid lwt ~map t uid)

  let of_offset_with_path ~map t ~path raw ~cursor =
    let map fd ~pos len =
      inj (map fd ~pos len) in
    prj (Carton.Dec.of_offset_with_path lwt ~map t ~path raw ~cursor)

  type 'uid digest = 'uid Carton.Dec.digest

  let uid_of_offset ~map ~digest t raw ~cursor =
    let map fd ~pos len =
      inj (map fd ~pos len) in
    prj (Carton.Dec.uid_of_offset lwt ~map ~digest t raw ~cursor)

  let uid_of_offset_with_source ~map ~digest t ~kind raw ~depth ~cursor =
    let map fd ~pos len =
      inj (map fd ~pos len) in
    prj (Carton.Dec.uid_of_offset_with_source lwt ~map ~digest t ~kind raw ~depth ~cursor)

  type 'uid oracle = 'uid Carton.Dec.oracle

  module Verify (Uid : Carton.UID) = struct
    include Carton.Dec.Verify (Uid) (Lwt_scheduler) (Lwt_io)

    let verify ~threads ~map ~oracle t ~matrix =
      let map fd ~pos len =
        inj (map fd ~pos len) in
      verify ~threads ~map ~oracle t ~matrix
  end
  module Ip (Uid : Carton.UID) =
    Carton.Dec.Ip (Lwt_scheduler) (Lwt_io) (Uid)
end

module Enc = struct
  type 'uid entry = 'uid Carton.Enc.entry
  type 'uid delta = 'uid Carton.Enc.delta =
    | From of 'uid | Zero

  let make_entry ~kind ~length ?preferred ?delta uid =
    Carton.Enc.make_entry ~kind ~length ?preferred ?delta uid

  let length entry = Carton.Enc.length entry

  type 'uid q = 'uid Carton.Enc.q
  type 'uid p = 'uid Carton.Enc.p
  type 'uid patch = 'uid Carton.Enc.patch

  type 'uid load = 'uid -> Dec.v Lwt.t
  type 'uid find = 'uid -> int option Lwt.t

  type 'uid uid = 'uid Carton.Enc.uid =
    { uid_ln : int
    ; uid_rw : 'uid -> string }

  let target_to_source target = Carton.Enc.target_to_source target
  let target_uid target = Carton.Enc.target_uid target

  let entry_to_target ~load entry =
    let load uid =
      inj (load uid) in
    prj (Carton.Enc.entry_to_target lwt ~load entry)

  let apply ~load ~uid_ln ~source ~target =
    let load uid =
      inj (load uid) in
    prj (Carton.Enc.apply lwt ~load ~uid_ln ~source ~target)

  module type VERBOSE = Carton.Enc.VERBOSE with type 'a fiber = 'a Lwt.t
  module type UID = Carton.Enc.UID

  module Delta (Uid : UID) (Verbose : VERBOSE) = struct
    include Carton.Enc.Delta (Lwt_scheduler) (Lwt_io) (Uid) (Verbose)

    let delta ~threads ~weight ~uid_ln matrix =
      let threads = List.map (fun load -> (fun uid -> inj (load uid))) threads in
      delta ~threads ~weight ~uid_ln matrix
  end

  module N = struct
    include Carton.Enc.N

    let encoder ~b ~load target =
      let load uid =
        inj (load uid) in
      prj (encoder lwt ~b ~load target)
  end

  type b = Carton.Enc.b =
    { i : Bigstringaf.t
    ; q : De.Queue.t
    ; w : De.window
    ; o : Bigstringaf.t }

  let header_of_pack ~length buf off len =
    Carton.Enc.header_of_pack ~length buf off len

  let encode_target ~b ~find ~load ~uid target ~cursor =
    let load uid = inj (load uid) in
    let find uid = inj (find uid) in
    prj (Carton.Enc.encode_target lwt ~b ~find ~load ~uid target ~cursor)
end
