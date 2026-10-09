import { useQueryClient } from "@tanstack/react-query";
import { LogOutIcon } from "lucide-react";
import { useNavigate } from "react-router-dom";

import { logout } from "@/api/generated";

export function LogoutButton({ className }: { className?: string }) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  async function onClick() {
    await logout();
    // What was loaded under the session must not stay on a shared computer.
    queryClient.clear();
    navigate("/login");
  }

  return (
    <button type="button" className={className} onClick={onClick}>
      <LogOutIcon className="size-4" />
      Выйти
    </button>
  );
}
