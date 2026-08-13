import { okfConvenienceViews } from '../generated/ficha-okf.views';

export function getOkfConvenienceView(name: string) {
  const view = okfConvenienceViews.find((item) => item.name === name);
  if (!view) throw new Error(`Unknown OKF convenience view: ${name}`);
  return view;
}

export { okfConvenienceViews };
