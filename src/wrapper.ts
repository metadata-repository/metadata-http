import { InternalServerError } from '#modules/metadata-error/src/errors/internal-server-error.js';
import { MetadataError } from '#modules/metadata-error/src/metadata-error.js';
import type Router from 'find-my-way';
import { type IncomingHttpHeaders } from 'node:http';
import { Readable } from 'node:stream';
import consumers from 'node:stream/consumers';
import { pipeline } from 'node:stream/promises';

export function wrapper(
  handler: (
    req: {
      body: {
        stream: ReadableStream;
        arrayBuffer(): Promise<ArrayBuffer>;
        blob(): Promise<Blob>;
        buffer(): Promise<Buffer>;
        json(): Promise<unknown>;
        text(): Promise<string>;
      };
      headers: IncomingHttpHeaders;
      params: Record<string, string | undefined>;
      searchParams: Record<string, string>;
    },
    store: any
  ) => Promise<Response>
) {
  return async (
    ...[req, res, params, store, searchParams]: Parameters<
      Router.Handler<Router.HTTPVersion.V1>
    >
  ) => {
    try {
      const result = await handler(
        {
          body: {
            stream: Readable.toWeb(req),
            arrayBuffer: () => consumers.arrayBuffer(req),
            blob: () => consumers.blob(req),
            buffer: () => consumers.buffer(req),
            json: () => consumers.json(req),
            text: () => consumers.text(req)
          },
          headers: req.headers,
          params,
          searchParams
        },
        store
      );

      res.writeHead(
        result.status,
        Object.fromEntries(result.headers.entries())
      );

      if (result.body) {
        await pipeline(Readable.fromWeb(result.body), res);
        return res;
      }

      return res.end();
    } catch (err) {
      if (err instanceof MetadataError) {
        return res
          .writeHead(err.status, {
            'content-type': 'application/problem+json'
          })
          .end(JSON.stringify(err.withInstance(req.url)));
      }

      return res
        .writeHead(500, {
          'content-type': 'application/problem+json'
        })
        .end(
          JSON.stringify(
            new InternalServerError(undefined, undefined, String(req.url))
          )
        );
    }
  };
}
