type elt =
  | Kind
  | String of string
  | Uid
  | Percent

type t = elt list

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
  match Angstrom.parse_string parser s with
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
