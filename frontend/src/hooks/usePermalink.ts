/**
 * パーマリンクコピー機能の共通フック
 *
 * DateSelector / GraphDisplay に丸ごと重複していた
 * copyPermalink / fallbackCopyToClipboard / showCopyNotification を統合。
 *
 * 使い方:
 *   const { notificationRef, notificationClassName, copyPermalink } = usePermalink()
 *   ...
 *   <div ref={notificationRef} className={notificationClassName}></div>
 */

import { useCallback, useRef } from "react";
import styles from "../components/GraphDisplay.module.css";

interface UsePermalinkResult {
    notificationRef: React.RefObject<HTMLDivElement>;
    notificationClassName: string;
    copyPermalink: (elementId: string) => void;
}

export function usePermalink(): UsePermalinkResult {
    const notificationRef = useRef<HTMLDivElement>(null);

    const showCopyNotification = useCallback((message: string) => {
        if (!notificationRef.current) return;

        notificationRef.current.textContent = message;
        notificationRef.current.classList.add(styles.show);

        setTimeout(() => {
            notificationRef.current?.classList.remove(styles.show);
        }, 3000);
    }, []);

    const fallbackCopyToClipboard = useCallback(
        (text: string, elementId: string) => {
            const textArea = document.createElement("textarea");
            textArea.value = text;
            textArea.style.cssText = "position:fixed;left:-9999px";
            document.body.appendChild(textArea);
            textArea.focus();
            textArea.select();

            try {
                if (document.execCommand("copy")) {
                    showCopyNotification("パーマリンクをコピーしました");
                    window.history.pushState(null, "", "#" + elementId);
                } else {
                    showCopyNotification("コピーに失敗しました");
                }
            } catch {
                showCopyNotification("コピーに失敗しました");
            } finally {
                document.body.removeChild(textArea);
            }
        },
        [showCopyNotification]
    );

    const copyPermalink = useCallback(
        (elementId: string) => {
            const permalink = window.location.origin + window.location.pathname + "#" + elementId;

            if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
                navigator.clipboard
                    .writeText(permalink)
                    .then(() => {
                        showCopyNotification("パーマリンクをコピーしました");
                        window.history.pushState(null, "", "#" + elementId);
                    })
                    .catch(() => fallbackCopyToClipboard(permalink, elementId));
            } else {
                fallbackCopyToClipboard(permalink, elementId);
            }
        },
        [showCopyNotification, fallbackCopyToClipboard]
    );

    return {
        notificationRef,
        notificationClassName: styles.copyNotification,
        copyPermalink,
    };
}
