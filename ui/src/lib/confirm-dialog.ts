export type ConfirmDestructiveOptions = {
  title?: string;
  message: string;
  confirmLabel?: string;
  cancelLabel?: string;
  dangerous?: boolean;
};

export type ConfirmDialogHandler = (options: ConfirmDestructiveOptions) => Promise<boolean>;

let confirmHandler: ConfirmDialogHandler | null = null;

export function registerConfirmDialog(handler: ConfirmDialogHandler): void {
  confirmHandler = handler;
}

export function unregisterConfirmDialog(handler: ConfirmDialogHandler): void {
  if (confirmHandler === handler) {
    confirmHandler = null;
  }
}

export function confirmDestructive(
  messageOrOptions: string | ConfirmDestructiveOptions,
): Promise<boolean> {
  if (!confirmHandler) {
    return Promise.resolve(false);
  }

  if (typeof messageOrOptions === "string") {
    return confirmHandler({
      title: "Confirm action",
      message: messageOrOptions,
      confirmLabel: "Delete",
      cancelLabel: "Cancel",
      dangerous: true,
    });
  }

  return confirmHandler({
    title: messageOrOptions.title ?? "Confirm action",
    message: messageOrOptions.message,
    confirmLabel: messageOrOptions.confirmLabel ?? "Delete",
    cancelLabel: messageOrOptions.cancelLabel ?? "Cancel",
    dangerous: messageOrOptions.dangerous ?? true,
  });
}
