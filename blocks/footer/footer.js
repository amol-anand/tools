import { getMetadata } from '../../scripts/aem.js';
import { loadFragment } from '../fragment/fragment.js';

/**
 * loads and decorates the footer
 * @param {Element} block The footer block element
 */
export default async function decorate(block) {
  const footerMeta = getMetadata('footer');
  block.textContent = '';

  // load footer fragment
  const footerPath = footerMeta.footer || '/footer';
  const fragment = await loadFragment(footerPath);

  // decorate footer DOM
  const footer = document.createElement('div');
  while (fragment.firstElementChild) footer.append(fragment.firstElementChild);

  block.append(footer);
}

function addStyles(path) {
  const link = document.createElement('link');
  link.setAttribute('rel', 'stylesheet');
  link.href = path;
  return link;
}

// expose as a web component
class AEMFooterWebComponent extends HTMLElement {
  // connect component
  async connectedCallback() {
    const shadow = this.attachShadow({ mode: 'open' });
    await decorate(shadow);
    shadow.prepend(addStyles('/blocks/footer/footer.css'));
    shadow.prepend(addStyles('/styles/styles.css'));
    shadow.prepend(addStyles('/styles/fonts.css'));
  }
}

// register component
customElements.define('aem-footer', AEMFooterWebComponent);
