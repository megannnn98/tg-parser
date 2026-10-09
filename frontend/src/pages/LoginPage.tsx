import { type FormEvent, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";

import { login } from "@/api/generated";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { errorMessage } from "@/lib/api";
import { safeNext } from "@/lib/auth";

function failure(response: Response | undefined, error: unknown): string {
  if (response?.status === 401) {
    return "Неверный пароль";
  }
  if (response?.status === 429) {
    const seconds = Number(response.headers.get("Retry-After"));
    const wait = seconds > 0 ? ` Повторите через ${Math.ceil(seconds / 60)} мин.` : "";
    return `Слишком много неверных паролей.${wait}`;
  }
  return errorMessage(error, response?.status);
}

export function LoginPage() {
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const [password, setPassword] = useState("");
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function onSubmit(event: FormEvent) {
    event.preventDefault();
    setSending(true);
    setError(null);
    try {
      const result = await login({ body: { password } });
      if (result.response?.ok) {
        navigate(safeNext(params.get("next")), { replace: true });
        return;
      }
      setPassword("");
      setError(failure(result.response, result.error));
    } catch {
      setError("Сервер не ответил");
    }
    setSending(false);
  }

  return (
    <Card className="mx-auto mt-16 max-w-sm">
      <CardHeader>
        <CardTitle>Вход</CardTitle>
      </CardHeader>
      <CardContent>
        <form className="space-y-3" onSubmit={onSubmit}>
          <div className="space-y-1">
            <Label htmlFor="password">Пароль</Label>
            <Input
              id="password"
              type="password"
              autoComplete="current-password"
              autoFocus
              required
              value={password}
              onChange={(event) => setPassword(event.target.value)}
            />
          </div>
          {error ? <p className="text-sm text-destructive">{error}</p> : null}
          <Button type="submit" className="w-full" disabled={sending}>
            Войти
          </Button>
        </form>
      </CardContent>
    </Card>
  );
}
