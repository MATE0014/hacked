import * as React from "react"
import { cva, type VariantProps } from "class-variance-authority"
import { Slot } from "radix-ui"

import { cn } from "@/lib/utils"

const buttonVariants = cva(
  "group/button relative isolate inline-flex shrink-0 items-center justify-center overflow-hidden whitespace-nowrap rounded-full border border-brand-teal/60 bg-transparent px-6 py-3 font-heading text-base font-semibold tracking-wide text-brand-teal transition outline-none before:pointer-events-none before:absolute before:inset-0 before:bg-linear-to-r before:from-white/0 before:via-white/20 before:to-white/0 before:opacity-0 before:transition-opacity disabled:pointer-events-none disabled:opacity-50 hover:border-brand-teal hover:bg-brand-teal/10 hover:text-[#8CF2E5] hover:before:opacity-100 focus-visible:ring-2 focus-visible:ring-brand-teal/35 [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-4",
  {
    variants: {
      variant: {
        default: "",
        outline:
          "",
        secondary:
          "",
        ghost:
          "rounded-xl border-transparent bg-transparent px-3 py-2 text-zinc-300 shadow-none hover:bg-white/8 hover:text-white",
        destructive:
          "border-red-400/35 bg-linear-to-b from-red-500/25 to-red-600/15 text-red-100 hover:border-red-300/45 hover:from-red-500/35 hover:to-red-600/20 focus-visible:ring-red-500/30",
        link: "h-auto rounded-none border-0 bg-transparent px-0 py-0 font-medium tracking-normal text-zinc-200 underline-offset-4 hover:text-white hover:underline",
      },
      size: {
        default: "",
        xs: "h-8 rounded-full px-3 py-1.5 text-xs",
        sm: "h-9 rounded-full px-4 py-2 text-sm",
        lg: "h-11 rounded-full px-7 py-3 text-base",
        icon: "size-9 rounded-full p-0",
        "icon-xs":
          "size-7 rounded-full p-0 [&_svg:not([class*='size-'])]:size-3",
        "icon-sm":
          "size-8 rounded-full p-0",
        "icon-lg": "size-10 rounded-full p-0",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  }
)

function Button({
  className,
  variant = "default",
  size = "default",
  asChild = false,
  ...props
}: React.ComponentProps<"button"> &
  VariantProps<typeof buttonVariants> & {
    asChild?: boolean
  }) {
  const Comp = asChild ? Slot.Root : "button"

  return (
    <Comp
      data-slot="button"
      data-variant={variant}
      data-size={size}
      className={cn(buttonVariants({ variant, size, className }))}
      {...props}
    />
  )
}

export { Button, buttonVariants }
