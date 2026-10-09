import { Route, Routes, useLocation } from "react-router-dom";

import { LogoutButton } from "@/components/LogoutButton";
import { LoginPage } from "@/pages/LoginPage";
import { NotFoundPage } from "@/pages/NotFoundPage";
import { ProfilePage } from "@/pages/ProfilePage";
import { UsersPage } from "@/pages/UsersPage";

/** The same addresses as the Jinja pages had, so old links keep working. */
export function App() {
  const onLogin = useLocation().pathname === "/login";
  return (
    <main className="mx-auto max-w-6xl px-4 py-6">
      {onLogin ? null : (
        <div className="flex justify-end">
          <LogoutButton />
        </div>
      )}
      <Routes>
        <Route index element={<UsersPage />} />
        <Route path="login" element={<LoginPage />} />
        <Route path="users/:tgId" element={<ProfilePage />} />
        <Route path="*" element={<NotFoundPage />} />
      </Routes>
    </main>
  );
}
