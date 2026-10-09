import { Route, Routes } from "react-router-dom";

import { NotFoundPage } from "@/pages/NotFoundPage";
import { ProfilePage } from "@/pages/ProfilePage";
import { UsersPage } from "@/pages/UsersPage";

/** The same addresses as the Jinja pages had, so old links keep working. */
export function App() {
  return (
    <main className="mx-auto max-w-6xl px-4 py-6">
      <Routes>
        <Route index element={<UsersPage />} />
        <Route path="users/:tgId" element={<ProfilePage />} />
        <Route path="*" element={<NotFoundPage />} />
      </Routes>
    </main>
  );
}
