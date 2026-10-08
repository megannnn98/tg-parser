import { Link } from "react-router-dom";

export function NotFoundPage() {
  return (
    <p className="text-sm">
      Страница не найдена.{" "}
      <Link to="/" className="text-primary hover:underline">
        Все пользователи
      </Link>
    </p>
  );
}
