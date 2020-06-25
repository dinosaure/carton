type elt =
  | Kind
  | String of string
  | Uid
  | Percent

type t = elt list

module Pretty = struct
  type ('uid, 'x) g =
    | Kind : ('uid, [ `A | `B | `C | `D ]) g
    | String : string -> ('uid, string) g
    | Uid : ('uid, 'uid) g
    | Percent : ('uid, percent) g
    | Product : ('uid, 'a) g * ('uid, 'b) g -> ('uid, 'a * 'b) g
    | End : ('Uid, epsilon) g
  and percent = [ `x25 ]
  and epsilon = unit
  and 'uid v = V : ('uid, 'v) g -> 'uid v

  let concat
    : elt -> ('uid, 'x) g -> 'uid v
    = fun fformat t -> match fformat with
      | Kind -> V (Product (Kind, t))
      | Uid -> V (Product (Uid, t))
      | String v -> V (Product (String v, t))
      | Percent -> V (Product (Percent, t))

  let make fformat =
    List.fold_right (fun x (V v) -> concat x v) fformat (V End)

  let kind_of_string : cursor:int -> string -> int * [ `A | `B | `C | `D ]
    = fun ~cursor x ->
      if String.length x - cursor > 0
      then match x.[cursor] with
        | 'a' -> cursor + 1, `A
        | 'b' -> cursor + 1, `B
        | 'c' -> cursor + 1, `C
        | 'd' -> cursor + 1, `D
        | _ -> Fmt.invalid_arg "kind_of_string"
      else Fmt.invalid_arg "kind_of_string"

  type 'uid uid_wr = cursor:int -> string -> int * 'uid

  let rec eval
    : type uid x. uid_wr:uid uid_wr -> (uid, x) g -> int -> string -> (x * int) option
    = fun ~uid_wr -> function
      | Kind -> fun cursor x ->
        ( try let cursor, v = kind_of_string ~cursor x in Some (v, cursor)
          with _ -> None )
      | Uid -> fun cursor x ->
        ( try let cursor, v = uid_wr ~cursor x in Some (v, cursor)
          with _ -> None )
      | String p ->
        fun cursor x ->
          let p' = String.sub x cursor (String.length p) in
          if String.equal p p' then Some (p, cursor + String.length p) else None
      | Percent -> fun cursor x ->
        if x.[cursor] = '%'
        then Some (`x25, succ cursor)
        else None
      | Product (a, b) ->
        fun cursor x ->
          ( match eval ~uid_wr a cursor x with
            | None -> None
            | Some (va, cursor) -> match eval ~uid_wr b cursor x with
              | Some (vb, cursor) -> Some ((va, vb), cursor)
              | None -> None )
      | End -> fun cursor x ->
        if cursor >= String.length x then Some ((), cursor) else None

  let rec choose_uid
    : type uid x. (uid, x) g -> x -> uid option
    = fun t v -> match t, v with
      | Uid, uid -> Some uid
      | Kind, _ -> None
      | String _, _ -> None
      | Percent, _ -> None
      | Product (ta, tb), (va, vb) ->
        ( match choose_uid ta va with
          | Some _ as v -> v
          | None -> match choose_uid tb vb with
            | Some _ as v -> v
            | None -> None )
      | End, _ -> None

  let rec choose_kind
    : type uid x. (uid, x) g -> x -> [ `A | `B | `C | `D ] option
    = fun t v -> match t, v with
      | Uid, _ -> None
      | Kind, kind -> Some kind
      | String _, _ -> None
      | Percent, _ -> None
      | Product (ta, tb) , (va, vb) ->
        ( match choose_kind ta va with
          | Some _ as v -> v
          | None -> match choose_kind tb vb with
            | Some _ as v -> v
            | None -> None )
      | End, _ -> None

  let extract ~uid_wr fformat s =
    let ( >>= ) x f = match x with
      | Some x -> f x
      | None -> None in

    let V g = make fformat in
    match eval ~uid_wr g 0 s with
    | Some (v, _) ->
      choose_uid g v
      >>= fun uid -> choose_kind g v
      >>= fun kind -> Some (uid, kind)
    | None -> None
end

let parse s =
  let is_not_percent x = x <> '%' in

  let parser =
    let open Angstrom in
    let v = char '%' *> peek_char >>= function
      | Some 'k' -> char 'k' *> return Kind
      | Some 'u' -> char 'u' *> return Uid
      | Some '%' -> char '%' *> return Percent
      | _ -> fail "Invalid format" in
    let e = take_while1 is_not_percent >>| fun v -> String v in
    many (e <|> v) in
  match Angstrom.parse_string ~consume:Angstrom.Consume.All parser s with
  | Ok v -> Ok v
  | Error _ -> Rresult.R.error_msgf "Invalid format: %s" s

let pp ppf lst =
  let lst = List.map
      (function
        | Kind -> "%k"
        | String v -> v
        | Uid -> "%u"
        | Percent -> "%%")
      lst in
  Fmt.pf ppf "%S" (String.concat "" lst)

let format ~pp_kind ~pp_uid fmt ppf (kind, uid) =
  let lst = List.map
      (function
        | Kind -> Fmt.strf "%a" pp_kind kind
        | String v -> v
        | Uid -> Fmt.strf "%a" pp_uid uid
        | Percent -> "%")
      fmt in
  Fmt.pf ppf "%s" (String.concat "" lst)

let scan ~uid_wr fformat s = Pretty.extract ~uid_wr fformat s
