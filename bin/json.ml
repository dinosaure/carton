type value =
  [ `Null | `Bool of bool | `String of string | `Float of float ]

let rec flat_obj acc = function
  | [] -> acc
  | (name, v) :: r ->
    let vs = flat_value (`Name name :: acc) v in
    flat_obj vs r

and flat_arr acc = function
  | [] -> acc
  | x :: r ->
    let xs = flat_value acc x in
    flat_arr xs r

and flat_value acc = function
  | #value as v -> v :: acc
  | `O lst -> let obj = flat_obj (`Os :: acc) lst in `Oe :: obj
  | `A lst -> let arr = flat_arr (`As :: acc) lst in `Ae :: arr

and flat v = List.rev (flat_value [] v)

let encode flush v =
  let encoder = Jsonm.encoder `Manual in
  let buf = Bytes.create De.io_buffer_size in

  let rec partial l = function
    | `Ok -> encode l
    | `Partial ->
      let len = Bytes.length buf - Jsonm.Manual.dst_rem encoder in
      flush buf 0 len ;
      Jsonm.Manual.dst encoder buf 0 (Bytes.length buf) ;
      partial l (Jsonm.encode encoder `Await)
  and encode = function
    | [] -> finish (Jsonm.encode encoder `End)
    | x :: r -> partial r (Jsonm.encode encoder (`Lexeme x))
  and finish = function
    | `Ok ->
      let len = Bytes.length buf - Jsonm.Manual.dst_rem encoder in
      flush buf 0 len
    | `Partial ->
      let len = Bytes.length buf - Jsonm.Manual.dst_rem encoder in
      flush buf 0 len ;
      Jsonm.Manual.dst encoder buf 0 (Bytes.length buf) ;
      finish (Jsonm.encode encoder `Await) in
  Jsonm.Manual.dst encoder buf 0 (Bytes.length buf) ;
  encode (flat v)
