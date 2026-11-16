import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  useUser,
  useAddFunds,
  useConfig,
  useCreatePayment,
  useProcessPayment,
  useCreateBillingPortalSession,
} from "@/api/control-layer";
import { toast } from "sonner";
import { useSettings, useOrganizationContext } from "@/contexts";
import { TransactionHistory } from "@/components/features/cost-management/CostManagement/TransactionHistory.tsx";
import { AutoTopupSection } from "@/components/features/cost-management/CostManagement/AutoTopupSection.tsx";
import { AddFundsModal } from "@/components/modals/AddCreditsModal/AddCreditsModal";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { CheckCircle2, XCircle, Loader2 } from "lucide-react";
import type { DisplayUser } from "@/types/display";

export function CostManagement() {
  const [searchParams] = useSearchParams();
  const { isFeatureEnabled, settings } = useSettings();
  const isDemoMode = isFeatureEnabled("demo");
  const { data: config } = useConfig();
  const { activeOrganizationId, activeOrganization } =
    useOrganizationContext();
  const isOrgAdmin =
    activeOrganization?.role === "owner" ||
    activeOrganization?.role === "admin";
  const canManageOrgBilling = !activeOrganizationId || isOrgAdmin;

  const [showSuccessModal, setShowSuccessModal] = useState(false);
  const [showCancelledModal, setShowCancelledModal] = useState(false);
  const [showAddFundsModal, setShowAddFundsModal] = useState(false);
  const filterUserId = searchParams.get("user");

  const billingUserId = filterUserId || activeOrganizationId || undefined;

  const { data: currentUser, refetch: refetchCurrentUser } = useUser("current");
  const { data: displayUser, refetch: refetchDisplayUser } = useUser(
    billingUserId || "current",
  );

  const addFundsMutation = useAddFunds();
  const createPaymentMutation = useCreatePayment();
  const createBillingPortalMutation = useCreateBillingPortalSession();
  const processPaymentMutation = useProcessPayment({
    onSuccess: () => {
      setTimeout(() => {
        setShowSuccessModal(false);
        refetchCurrentUser();
        refetchDisplayUser();
      }, 2000);
    },
  });

  const canManageFunds = currentUser?.roles?.some(
    (role) => role === "PlatformManager" || role === "BillingManager",
  );

  const handleAddFundsSuccess = () => {
    refetchCurrentUser();
    refetchDisplayUser();
  };

  const handlePurchaseFundsExternal = () => {
    const paymentProviderUrl = settings.paymentProviderUrl;
    if (!paymentProviderUrl) {
      return;
    }

    const callbackUrl = `${window.location.origin}${window.location.pathname}${window.location.search}`;

    const redirectUrl = `${paymentProviderUrl}?callback=${encodeURIComponent(callbackUrl)}`;
    window.location.href = redirectUrl;
  };

  useEffect(() => {
    if (isDemoMode) return;

    const urlParams = new URLSearchParams(window.location.search);
    const paymentStatus = urlParams.get("payment");
    const sessionId = urlParams.get("session_id");

    if (paymentStatus === "success" && sessionId) {
      setShowSuccessModal(true);
      processPaymentMutation.mutate(sessionId);

      window.history.replaceState({}, "", window.location.pathname);
    } else if (paymentStatus === "cancelled" && sessionId) {
      setShowCancelledModal(true);
      window.history.replaceState({}, "", window.location.pathname);
    } else if (paymentStatus === "success" && !sessionId) {
      toast.success("Payment completed! Your balance has been updated.");
      window.history.replaceState({}, "", window.location.pathname);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isDemoMode]);

  useEffect(() => {
    if (processPaymentMutation.isSuccess && showSuccessModal) {
      const timer = setTimeout(() => {
        setShowSuccessModal(false);
      }, 2000);
      return () => clearTimeout(timer);
    }
  }, [processPaymentMutation.isSuccess, showSuccessModal]);

  const handlePurchaseFunds = async () => {
    if (isDemoMode) {
      const fundAmount = 100.0;
      try {
        await addFundsMutation.mutateAsync({
          source_id: displayUser?.id || "",
          user_id: displayUser?.id || "",
          amount: fundAmount,
          description: "Funds purchase - Demo top up",
        });
        toast.success(`Added $${fundAmount.toFixed(2)}`);
      } catch {
        toast.error("Failed to add funds");
      }
    } else if (config?.payment_enabled) {
      try {
        const data = await createPaymentMutation.mutateAsync(
          billingUserId || undefined,
        );
        if (data.url) {
          window.location.href = data.url;
        } else {
          toast.error("Failed to get checkout URL");
        }
      } catch {
        toast.error("Failed to transfer to payment provider.");
      }
    } else {
      toast.error("Payment processing is not configured");
    }
  };

  const handleGiftFunds = () => {
    setShowAddFundsModal(true);
  };

  const handleBillingPortal = async () => {
    try {
      const data = await createBillingPortalMutation.mutateAsync();
      if (data.url) {
        window.location.href = data.url;
      } else {
        toast.error("Failed to get billing portal URL");
      }
    } catch (error) {
      console.error("Failed to create billing portal session:", error);
      toast.error("Failed to access billing portal");
    }
  };

  const addFundsConfig = (() => {
    if (!canManageOrgBilling) return undefined;

    const hasPaymentEnabled = isDemoMode || !!config?.payment_enabled;
    const hasPaymentProvider = !!settings.paymentProviderUrl;
    const hasCustomerId = !!displayUser?.has_payment_provider_id;

    if (!hasPaymentEnabled && !hasPaymentProvider) {
      return canManageFunds
        ? {
            type: "admin-only" as const,
            onGiftFunds: handleGiftFunds,
          }
        : undefined;
    }

    if (canManageFunds || hasCustomerId) {
      return {
        type: "split" as const,
        onPurchaseFunds: hasPaymentProvider
          ? handlePurchaseFundsExternal
          : handlePurchaseFunds,
        onGiftFunds: canManageFunds ? handleGiftFunds : undefined,
        onBillingPortal: hasCustomerId ? handleBillingPortal : undefined,
      };
    } else {
      return {
        type: "purchase-only" as const,
        onPurchaseFunds: hasPaymentProvider
          ? handlePurchaseFundsExternal
          : handlePurchaseFunds,
      };
    }
  })();

  const showAutoTopupSection =
    (isDemoMode || !!config?.payment_enabled) &&
    displayUser &&
    (!filterUserId || filterUserId === activeOrganizationId) &&
    canManageOrgBilling;

  const autoTopupElement = showAutoTopupSection ? (
    <AutoTopupSection
      user={displayUser!}
      onSuccess={() => {
        refetchCurrentUser();
        refetchDisplayUser();
      }}
    />
  ) : undefined;

  return (
    <div className="p-6">
      {currentUser && displayUser && (
        <>
          <TransactionHistory
            userId={billingUserId || currentUser.id}
            addFundsConfig={addFundsConfig}
            showCard={false}
            filterUserId={filterUserId || undefined}
            headerExtra={autoTopupElement}
          />
          { }
          {canManageFunds && (
            <AddFundsModal
              isOpen={showAddFundsModal}
              onClose={() => setShowAddFundsModal(false)}
              targetUser={displayUser as DisplayUser}
              onSuccess={handleAddFundsSuccess}
            />
          )}
        </>
      )}

      { }
      <Dialog open={showSuccessModal} onOpenChange={setShowSuccessModal}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              {processPaymentMutation.isPending ? (
                <>
                  <Loader2 className="h-6 w-6 animate-spin text-blue-500" />
                  Processing Payment
                </>
              ) : processPaymentMutation.isError ? (
                <>
                  <XCircle className="h-6 w-6 text-red-500" />
                  Payment Processing Failed
                </>
              ) : (
                <>
                  <CheckCircle2 className="h-6 w-6 text-green-500" />
                  Payment Successful
                </>
              )}
            </DialogTitle>
            <DialogDescription>
              {processPaymentMutation.isPending ? (
                "Processing your payment and updating your account balance..."
              ) : processPaymentMutation.isError ? (
                <div className="space-y-2">
                  <p>
                    Your payment has been captured but not yet applied to your
                    account.
                  </p>
                  <p className="text-sm text-gray-600">
                    Your balance should update automatically within a few
                    minutes. If it doesn't, please contact support.
                  </p>
                </div>
              ) : (
                <div className="space-y-2">
                  <p>
                    Thank you for your payment! Your account balance has been
                    updated.
                  </p>
                  <p className="text-sm text-gray-600">
                    You can now use your credits for API requests.
                  </p>
                </div>
              )}
            </DialogDescription>
          </DialogHeader>
          <div className="flex justify-end gap-2 mt-4">
            {processPaymentMutation.isPending ? (
              <Button
                variant="outline"
                onClick={() => setShowSuccessModal(false)}
              >
                Close (processing in background)
              </Button>
            ) : (
              <Button onClick={() => setShowSuccessModal(false)}>Close</Button>
            )}
          </div>
        </DialogContent>
      </Dialog>

      { }
      <Dialog open={showCancelledModal} onOpenChange={setShowCancelledModal}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <XCircle className="h-6 w-6 text-yellow-500" />
              Payment Cancelled
            </DialogTitle>
            <DialogDescription>
              <div className="space-y-2">
                <p>
                  Your payment was cancelled. No charges have been made to your
                  account.
                </p>
                <p className="text-sm text-gray-600">
                  You can try adding funds again whenever you're ready.
                </p>
              </div>
            </DialogDescription>
          </DialogHeader>
          <div className="flex justify-end gap-2 mt-4">
            <Button
              variant="outline"
              onClick={() => setShowCancelledModal(false)}
            >
              Close
            </Button>
            <Button
              onClick={() => {
                setShowCancelledModal(false);
                handlePurchaseFunds();
              }}
            >
              Try Again
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}
