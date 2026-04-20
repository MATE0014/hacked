"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { motion, AnimatePresence } from "framer-motion";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faChartLine,
  faChartPie,
  faChartBar,
  faGear,
  faRightFromBracket,
  faBars,
  faXmark,
} from "@fortawesome/free-solid-svg-icons";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Sheet,
  SheetContent,
  SheetTrigger,
} from "@/components/ui/sheet";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Separator } from "@/components/ui/separator";
import { useAuthStore } from "@/lib/auth-store";
import { useState } from "react";

const navItems = [
  { href: "/dashboard", label: "Dashboard", icon: faChartLine },
  { href: "/analyzer", label: "Analyzer", icon: faChartBar },
  { href: "/dashboard/analytics", label: "Analytics", icon: faChartPie },
  { href: "/dashboard/settings", label: "Status", icon: faGear },
];

function NavLink({
  item,
  isActive,
  collapsed,
}: {
  item: (typeof navItems)[0];
  isActive: boolean;
  collapsed?: boolean;
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Link
          href={item.href}
          className={`group relative flex items-center gap-3 rounded-xl px-3 py-2.5 text-sm font-medium transition-all duration-200 ${
            isActive
              ? "bg-brand-teal/15 text-white"
              : "text-[#938EA0] hover:bg-white/[0.04] hover:text-[#CAC4D7]"
          } ${collapsed ? "justify-center" : ""}`}
        >
          {isActive && (
            <motion.div
              layoutId="active-nav"
              className="absolute left-0 top-1/2 h-6 w-[3px] -translate-y-1/2 rounded-r-full bg-brand-teal"
              transition={{ type: "spring", stiffness: 380, damping: 30 }}
            />
          )}
          <FontAwesomeIcon
            icon={item.icon}
            className={`h-4 w-4 flex-shrink-0 ${
              isActive ? "text-brand-teal" : ""
            }`}
          />
          {!collapsed && <span>{item.label}</span>}
        </Link>
      </TooltipTrigger>
      {collapsed && (
        <TooltipContent side="right" sideOffset={8}>
          {item.label}
        </TooltipContent>
      )}
    </Tooltip>
  );
}

function SidebarContent({ collapsed }: { collapsed?: boolean }) {
  const pathname = usePathname();
  const router = useRouter();
  const logout = useAuthStore((state) => state.logout);

  const handleLogout = () => {
    logout();
    router.push("/login");
  };

  return (
    <div className="flex h-full flex-col">
      {/* Logo */}
      <div className={`flex items-center gap-2.5 px-4 py-6 ${collapsed ? "justify-center" : ""}`}>
        <div className="flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-lg bg-brand-teal">
          <FontAwesomeIcon icon={faChartLine} className="h-4 w-4 text-white" />
        </div>
        {!collapsed && (
          <span className="font-heading text-lg tracking-tight text-white">
            InsightFlow
          </span>
        )}
      </div>

      <Separator className="bg-white/[0.06]" />

      {/* Nav */}
      <nav className="flex-1 space-y-1 px-3 py-4">
        {navItems.map((item) => (
          <NavLink
            key={item.href}
            item={item}
            isActive={
              item.href === "/dashboard"
                ? pathname === "/dashboard"
                : pathname.startsWith(item.href)
            }
            collapsed={collapsed}
          />
        ))}
      </nav>

      <Separator className="bg-white/[0.06]" />

      {/* User */}
      <div className={`p-3 ${collapsed ? "flex justify-center" : ""}`}>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button
              className={`relative isolate flex w-full items-center gap-3 overflow-hidden rounded-xl px-3 py-2.5 text-left transition-colors before:pointer-events-none before:absolute before:inset-0 before:bg-linear-to-r before:from-white/0 before:via-white/20 before:to-white/0 before:opacity-0 before:transition-opacity hover:bg-white/[0.04] hover:before:opacity-100 ${
                collapsed ? "justify-center px-0" : ""
              }`}
            >
              <Avatar className="h-8 w-8 border border-white/10">
                <AvatarImage src="/user-no-av.png" alt="User avatar" />
                <AvatarFallback className="bg-brand-teal/20 text-xs text-brand-teal">
                  AF
                </AvatarFallback>
              </Avatar>
              {!collapsed && (
                <div className="min-w-0 flex-1">
                  <p className="truncate text-sm font-medium text-white">
                    Admin User
                  </p>
                  <p className="truncate text-xs text-[#938EA0]">
                    admin@flow.org
                  </p>
                </div>
              )}
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-48">
            <DropdownMenuItem onClick={handleLogout}>
              <FontAwesomeIcon icon={faRightFromBracket} className="mr-2 h-3 w-3" />
              Logout
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </div>
  );
}

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = usePathname();
  const [mobileOpen, setMobileOpen] = useState(false);

  const pageTitle = navItems.find((item) =>
    item.href === "/dashboard"
      ? pathname === "/dashboard"
      : pathname.startsWith(item.href)
  )?.label ?? "Dashboard";

  return (
    <div className="flex h-screen overflow-hidden bg-[#07080C]">
      {/* Desktop Sidebar */}
      <aside className="hidden w-[240px] flex-shrink-0 border-r border-white/[0.06] bg-[#0B0D14] lg:block">
        <SidebarContent />
      </aside>

      {/* Mobile Sidebar */}
      <Sheet open={mobileOpen} onOpenChange={setMobileOpen}>
        <SheetContent
          side="left"
          className="w-[260px] border-r border-white/[0.06] bg-[#0B0D14] p-0"
        >
          <SidebarContent />
        </SheetContent>
      </Sheet>

      {/* Main Content */}
      <div className="flex flex-1 flex-col overflow-hidden">
        {/* Top Header */}
        <header className="flex h-16 flex-shrink-0 items-center border-b border-white/[0.06] bg-[#0B0D14]/50 px-6 backdrop-blur-xl">
          <div className="flex items-center gap-4">
            <Button
              variant="ghost"
              size="icon"
              className="lg:hidden"
              onClick={() => setMobileOpen(true)}
            >
              <FontAwesomeIcon icon={faBars} className="h-4 w-4" />
            </Button>
            <h1 className="font-heading text-xl text-white">{pageTitle}</h1>
          </div>
        </header>

        {/* Page Content */}
        <main className="scrollbar-brand flex-1 overflow-y-auto">
          <AnimatePresence mode="wait">
            <motion.div
              key={pathname}
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -8 }}
              transition={{ duration: 0.25, ease: "easeOut" }}
              className="p-6 lg:p-8"
            >
              {children}
            </motion.div>
          </AnimatePresence>
        </main>
      </div>
    </div>
  );
}
