"use client";

import AnimatedCanvas from "@/components/animated-canvas";
import { Button } from "@/components/ui/button";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import Link from "next/link";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faEye, faEyeSlash } from "@fortawesome/free-regular-svg-icons";
import * as z from "zod";
import { useAuthStore } from "@/lib/auth-store";

const AUTH_EMAIL = "admin@flow.org";
const AUTH_PASSWORD = "ez4u@123";

const formSchema = z.object({
  email: z.string().email({ message: "Please enter a valid email." }),
  password: z.string().min(1, { message: "Password is required." }),
});

const LoginPage = () => {
  const router = useRouter();
  const [loginError, setLoginError] = useState<string | null>(null);
  const [showPassword, setShowPassword] = useState(false);
  const isAuthenticated = useAuthStore((state) => state.isAuthenticated);
  const hasHydrated = useAuthStore((state) => state.hasHydrated);
  const login = useAuthStore((state) => state.login);

  useEffect(() => {
    if (hasHydrated && isAuthenticated) {
      router.replace("/dashboard");
    }
  }, [hasHydrated, isAuthenticated, router]);

  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      email: "",
      password: "",
    },
  });

  const onSubmit = (values: z.infer<typeof formSchema>) => {
    const isValidUser =
      values.email === AUTH_EMAIL && values.password === AUTH_PASSWORD;

    if (!isValidUser) {
      setLoginError("Invalid email or password.");
      return;
    }

    setLoginError(null);
    login();
    router.push("/dashboard");
  };

  return (
    <div className="relative flex min-h-screen items-center justify-center overflow-hidden bg-brand-dark px-4">
      <AnimatedCanvas />
      <Card className="z-10 w-full max-w-sm border-white/10 bg-surface/90 backdrop-blur-md">
          <CardHeader className="text-center">
            <CardTitle className="text-3xl font-heading text-white">
              InsightFlow
            </CardTitle>
            <CardDescription>
              Sign in to your AI-powered analytics dashboard.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
              {loginError && (
                <Alert variant="destructive">
                  <AlertTitle>Login failed</AlertTitle>
                  <AlertDescription>{loginError}</AlertDescription>
                </Alert>
              )}

              <div className="space-y-2">
                <Label htmlFor="email">Email</Label>
                <Input
                  id="email"
                  placeholder="name@example.com"
                  {...form.register("email")}
                />
                {form.formState.errors.email && (
                  <p className="text-sm text-red-500">
                    {form.formState.errors.email.message}
                  </p>
                )}
              </div>
              <div className="space-y-2">
                <Label htmlFor="password">Password</Label>
                <div className="relative">
                  <Input
                    id="password"
                    type={showPassword ? "text" : "password"}
                    placeholder="Enter your password"
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
                  <p className="text-sm text-red-500">
                    {form.formState.errors.password.message}
                  </p>
                )}
              </div>
              <Button
                type="submit"
                className="w-full"
              >
                Login
              </Button>

              <p className="text-center text-sm text-muted-foreground">
                Need an account?{" "}
                <Link href="/signup" className="text-brand-teal hover:underline">
                  Sign up
                </Link>
              </p>
            </form>
          </CardContent>
        </Card>
    </div>
  );
};

export default LoginPage;