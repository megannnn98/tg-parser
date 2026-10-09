import { useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";

import { logout } from "@/api/generated";
import { Button } from "@/components/ui/button";

export function LogoutButton() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  async function onClick() {
    await logout();
    // What was loaded under the session must not stay on a shared computer.
    queryClient.clear();
    navigate("/login");
  }

  return (
    <Button variant="ghost" size="sm" onClick={onClick}>
      Выйти
    </Button>
  );
}
