"use client";

import AnimatedCanvas from "@/components/animated-canvas";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { zodResolver } from "@hookform/resolvers/zod";
import Link from "next/link";
import { useEffect, useState } from "react";
import { useForm } from "react-hook-form";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faEye, faEyeSlash } from "@fortawesome/free-regular-svg-icons";
import * as z from "zod";
import { useRouter } from "next/navigation";
import { useAuthStore } from "@/lib/auth-store";

const signupSchema = z
  .object({
    fullName: z
      .string()
      .min(2, { message: "Please enter your full name." })
      .max(100, { message: "Full name is too long." }),
    email: z.string().email({ message: "Please enter a valid email." }),
    confirmEmail: z.string().email({ message: "Please confirm with a valid email." }),
    password: z
      .string()
      .min(8, { message: "Password must be at least 8 characters." }),
    confirmPassword: z.string().min(1, { message: "Please confirm your password." }),
  })
  .superRefine((data, ctx) => {
    if (data.email !== data.confirmEmail) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "Emails do not match.",
        path: ["confirmEmail"],
      });
    }

    if (data.password !== data.confirmPassword) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "Passwords do not match.",
        path: ["confirmPassword"],
      });
    }
  });

type SignupValues = z.infer<typeof signupSchema>;

export default function SignupPage() {
  const router = useRouter();
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const isAuthenticated = useAuthStore((state) => state.isAuthenticated);
  const hasHydrated = useAuthStore((state) => state.hasHydrated);

  useEffect(() => {
    if (hasHydrated && isAuthenticated) {
      router.replace("/dashboard");
    }
  }, [hasHydrated, isAuthenticated, router]);

  const form = useForm<SignupValues>({
    resolver: zodResolver(signupSchema),
    defaultValues: {
      fullName: "",
      email: "",
      confirmEmail: "",
      password: "",
      confirmPassword: "",
    },
  });

  const onSubmit = (values: SignupValues) => {
    setSuccessMessage(
      `Sample signup submitted for ${values.fullName}. Connect this form to your backend next.`
    );
    form.reset();
  };

  return (
    <div className="relative flex min-h-screen items-center justify-center overflow-hidden bg-brand-dark px-4 py-8">
      <AnimatedCanvas />

      <Card className="z-10 w-full max-w-md border-white/10 bg-surface/90 backdrop-blur-md">
        <CardHeader className="text-center">
          <CardTitle className="text-3xl font-heading text-white">InsightFlow</CardTitle>
          <CardDescription>Create your account to start analyzing data.</CardDescription>
        </CardHeader>

        <CardContent>
          <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
            {successMessage && (
              <Alert>
                <AlertTitle>Signup complete</AlertTitle>
                <AlertDescription>{successMessage}</AlertDescription>
              </Alert>
            )}

            <div className="space-y-2">
              <Label htmlFor="fullName">Full Name</Label>
              <Input id="fullName" placeholder="Your full name" {...form.register("fullName")} />
              {form.formState.errors.fullName && (
                <p className="text-sm text-red-500">{form.formState.errors.fullName.message}</p>
              )}
            </div>

            <div className="space-y-2">
              <Label htmlFor="email">Email</Label>
              <Input id="email" type="email" placeholder="name@example.com" {...form.register("email")} />
              {form.formState.errors.email && (
                <p className="text-sm text-red-500">{form.formState.errors.email.message}</p>
              )}
            </div>

            <div className="space-y-2">
              <Label htmlFor="confirmEmail">Confirm Email</Label>
              <Input
                id="confirmEmail"
                type="email"
                placeholder="Confirm your email"
                {...form.register("confirmEmail")}
              />
              {form.formState.errors.confirmEmail && (
                <p className="text-sm text-red-500">{form.formState.errors.confirmEmail.message}</p>
              )}
            </div>

            <div className="space-y-2">
              <Label htmlFor="password">Password</Label>
              <div className="relative">
                <Input
                  id="password"
                  type={showPassword ? "text" : "password"}
                  placeholder="Create a password"
                  className="pr-20"
                  {...form.register("password")}
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-sm"
                  onClick={() => setShowPassword((prev) => !prev)}
                  className="absolute right-1 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                  aria-label={showPassword ? "Hide password" : "Show password"}
                >
                  <FontAwesomeIcon icon={showPassword ? faEyeSlash : faEye} />
                </Button>
              </div>
              {form.formState.errors.password && (
                <p className="text-sm text-red-500">{form.formState.errors.password.message}</p>
              )}
            </div>

            <div className="space-y-2">
              <Label htmlFor="confirmPassword">Confirm Password</Label>
              <div className="relative">
                <Input
                  id="confirmPassword"
                  type={showConfirmPassword ? "text" : "password"}
                  placeholder="Confirm your password"
                  className="pr-20"
                  {...form.register("confirmPassword")}
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-sm"
                  onClick={() => setShowConfirmPassword((prev) => !prev)}
                  className="absolute right-1 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                  aria-label={showConfirmPassword ? "Hide confirm password" : "Show confirm password"}
                >
                  <FontAwesomeIcon icon={showConfirmPassword ? faEyeSlash : faEye} />
                </Button>
              </div>
              {form.formState.errors.confirmPassword && (
                <p className="text-sm text-red-500">{form.formState.errors.confirmPassword.message}</p>
              )}
            </div>

            <Button type="submit" className="w-full">
              Create account
            </Button>

            <p className="text-center text-sm text-muted-foreground">
              Already have an account?{" "}
              <Link href="/login" className="text-brand-teal hover:underline">
                Login
              </Link>
            </p>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}
