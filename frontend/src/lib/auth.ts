const LOGIN = "/login";

/** Where to go after the login: only a page of this site, never the login itself.
 * The address comes from the query string, which anyone can put in a link. */
export function safeNext(value: string | null): string {
  if (!value || !value.startsWith("/") || value.startsWith("//") || value.startsWith("/\\")) {
    return "/";
  }
  const path = value.split("?")[0];
  return path === LOGIN ? "/" : value;
}

export function loginAddress(pathname: string, search: string): string {
  const here = pathname + search;
  return here === "/" ? LOGIN : `${LOGIN}?next=${encodeURIComponent(here)}`;
}

type BrowserLocation = Pick<Location, "pathname" | "search" | "assign">;

/** Any API answer 401 means the session is gone or was never there: the login page is
 * the only place to continue from. On the login page itself 401 is a wrong password. */
export function sendToLoginOn401(response: Response, location: BrowserLocation = window.location): Response {
  if (response.status === 401 && location.pathname !== LOGIN) {
    location.assign(loginAddress(location.pathname, location.search));
  }
  return response;
}
