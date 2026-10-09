import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import { ChevronLeft, ChevronRight, ExternalLink, X as CloseIcon } from "lucide-react";
import { chartSrc, tweetUrl, type ChartItem } from "./courseData";

/* Full-screen chart viewer that steps through a whole set (a lesson's
   examples, or the gallery's current filter) with the arrow keys. Portals to
   <body>: main.workspace carries a transform (CLAUDE.md gotcha 17). */
export function CourseLightbox({
  items,
  index,
  onIndex,
  onClose,
  onOpenLesson,
}: {
  items: ChartItem[];
  index: number;
  onIndex: (next: number) => void;
  onClose: () => void;
  onOpenLesson?: (lessonId: string) => void;
}) {
  const item = items[index];
  const [failed, setFailed] = useState(false);
  const count = items.length;

  useEffect(() => setFailed(false), [index]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
      else if (e.key === "ArrowRight" && count > 1) onIndex((index + 1) % count);
      else if (e.key === "ArrowLeft" && count > 1) onIndex((index - 1 + count) % count);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [index, count, onIndex, onClose]);

  if (!item) return null;
  const src = failed ? chartSrc(null, item.image, "large") : chartSrc(item.local, item.image, "large");
  const stop = (e: React.MouseEvent) => e.stopPropagation();

  return createPortal(
    <div className="course-lightbox" role="dialog" aria-modal="true" aria-label="Enlarged chart" onClick={onClose}>
      <button type="button" className="course-lightbox-close" onClick={onClose} aria-label="Close">
        <CloseIcon size={18} />
      </button>
      {count > 1 ? (
        <>
          <button
            type="button"
            className="course-lightbox-nav is-prev"
            aria-label="Previous chart"
            onClick={(e) => {
              stop(e);
              onIndex((index - 1 + count) % count);
            }}
          >
            <ChevronLeft size={22} />
          </button>
          <button
            type="button"
            className="course-lightbox-nav is-next"
            aria-label="Next chart"
            onClick={(e) => {
              stop(e);
              onIndex((index + 1) % count);
            }}
          >
            <ChevronRight size={22} />
          </button>
        </>
      ) : null}
      <img
        src={src}
        alt={item.caption}
        referrerPolicy="no-referrer"
        onClick={stop}
        onError={() => {
          if (!failed && item.local && item.image) setFailed(true);
        }}
      />
      <div className="course-lightbox-meta" onClick={stop}>
        <p>
          {count > 1 ? <span className="course-mono">{index + 1} / {count} · </span> : null}
          {item.date ? <span className="course-mono">{item.date} · </span> : null}
          {item.caption}
        </p>
        <p className="course-lightbox-links">
          {item.lessonId && item.lessonTitle && onOpenLesson ? (
            <button type="button" onClick={() => onOpenLesson(item.lessonId!)}>
              Open lesson: {item.lessonTitle}
            </button>
          ) : null}
          {item.tweet ? (
            <a href={tweetUrl(item.tweet)} target="_blank" rel="noreferrer noopener">
              Source tweet <ExternalLink size={12} />
            </a>
          ) : null}
          <a href={src} target="_blank" rel="noreferrer noopener">
            Open image <ExternalLink size={12} />
          </a>
        </p>
      </div>
    </div>,
    document.body,
  );
}
