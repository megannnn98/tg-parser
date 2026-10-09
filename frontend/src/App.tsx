import { Route, Routes, useLocation } from "react-router-dom";

import { Sidebar } from "@/components/layout/Sidebar";
import { LoginPage } from "@/pages/LoginPage";
import { NotFoundPage } from "@/pages/NotFoundPage";
import { ProfilePage } from "@/pages/ProfilePage";
import { UsersPage } from "@/pages/UsersPage";

/** The same addresses as the Jinja pages had, so old links keep working. */
export function App() {
  if (useLocation().pathname === "/login") {
    return (
      <main className="mx-auto max-w-6xl px-4 py-6">
        <LoginPage />
      </main>
    );
  }
  return (
    <div className="min-h-screen lg:flex">
      <Sidebar />
      <main className="min-w-0 flex-1 px-4 py-6 lg:px-8">
        <div className="mx-auto max-w-6xl">
          <Routes>
            <Route index element={<UsersPage />} />
            <Route path="users/:tgId" element={<ProfilePage />} />
            <Route path="*" element={<NotFoundPage />} />
          </Routes>
        </div>
      </main>
    </div>
  );
}
