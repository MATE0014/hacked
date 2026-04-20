import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";

type AuthState = {
  isAuthenticated: boolean;
  hasHydrated: boolean;
  setHasHydrated: (state: boolean) => void;
  login: () => void;
  logout: () => void;
};

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      isAuthenticated: false,
      hasHydrated: false,
      setHasHydrated: (state) => set({ hasHydrated: state }),
      login: () => set({ isAuthenticated: true }),
      logout: () => set({ isAuthenticated: false }),
    }),
    {
      name: "insightflow-auth",
      storage: createJSONStorage(() => localStorage),
      partialize: (state) => ({ isAuthenticated: state.isAuthenticated }),
      onRehydrateStorage: () => (state) => {
        state?.setHasHydrated(true);
      },
    }
  )
);
