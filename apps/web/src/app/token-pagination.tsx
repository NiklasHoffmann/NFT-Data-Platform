/**
 * Page links for the token archive. Plain anchors, so it works without JavaScript
 * and every page is a shareable URL. Long ranges collapse around the current page
 * rather than rendering hundreds of links.
 *
 * The href builder is injected so this stays independent of how the page encodes its
 * query string.
 */
export function TokenPagination(props: {
  page: number;
  pageCount: number;
  hrefForPage: (page: number) => string;
  label?: string;
}): React.JSX.Element | null {
  if (props.pageCount <= 1) {
    return null;
  }

  const pages = buildPaginationRange(props.page, props.pageCount);

  return (
    <nav className="token-pagination" aria-label={props.label ?? "Seiten"}>
      {props.page > 1 ? (
        <a href={props.hrefForPage(props.page - 1)} className="detail-link" rel="prev">
          Zurück
        </a>
      ) : (
        <span className="token-pagination__disabled" aria-hidden="true">
          Zurück
        </span>
      )}

      <ol className="token-pagination__pages">
        {pages.map((page, index) =>
          page === null ? (
            <li key={`gap-${index}`} className="token-pagination__gap" aria-hidden="true">
              …
            </li>
          ) : (
            <li key={page}>
              {page === props.page ? (
                <span className="token-pagination__current" aria-current="page">
                  {page}
                </span>
              ) : (
                <a href={props.hrefForPage(page)} className="token-pagination__page">
                  <span className="sr-only">Seite </span>
                  {page}
                </a>
              )}
            </li>
          )
        )}
      </ol>

      {props.page < props.pageCount ? (
        <a href={props.hrefForPage(props.page + 1)} className="detail-link" rel="next">
          Weiter
        </a>
      ) : (
        <span className="token-pagination__disabled" aria-hidden="true">
          Weiter
        </span>
      )}
    </nav>
  );
}

/**
 * First and last page always, plus a window around the current one. null marks an
 * elided stretch.
 */
export function buildPaginationRange(page: number, pageCount: number): Array<number | null> {
  const windowRadius = 1;
  const shown = new Set<number>([1, pageCount]);

  for (let candidate = page - windowRadius; candidate <= page + windowRadius; candidate += 1) {
    if (candidate >= 1 && candidate <= pageCount) {
      shown.add(candidate);
    }
  }

  const ordered = [...shown].sort((left, right) => left - right);
  const range: Array<number | null> = [];

  for (const [index, value] of ordered.entries()) {
    const previous = ordered[index - 1];

    if (previous !== undefined && value - previous > 1) {
      range.push(null);
    }

    range.push(value);
  }

  return range;
}
