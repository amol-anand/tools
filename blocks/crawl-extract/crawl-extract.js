/* eslint-disable no-console */
// function showResults() {
//   console.log(`results has ${results.length} items`);
//   results.forEach((result) => {
//     const resultDiv = document.createElement('div');
//     resultDiv.classList.add('result');
//     resultDiv.innerHTML = `
//       <h5>${result.url}</h5>
//       <ul>
//         ${result.values.map((content) => `<li>${content}</li>`).join('')}
//       </ul>
//     `;
//     resultsDiv.appendChild(resultDiv);
//   });
// }

async function crawlAndExtract() {
  const sitemapUrlsVal = document.getElementById('sitemapUrls').value;
  const pathVal = document.getElementById('path').value;
  const selectorsVal = document.getElementById('selectors').value;
  const resultsDiv = document.querySelector('.results');
  resultsDiv.classList.remove('hidden');
  const table = document.createElement('table');
  table.innerHTML = '<th>URL</th><th>Extracted Content</th>';
  resultsDiv.appendChild(table);
  if (sitemapUrlsVal && selectorsVal) {
    const sitemaps = sitemapUrlsVal.split(',');
    const selectors = selectorsVal.split(',');
    await sitemaps.forEach(async (sitemap) => {
      const sitemapUrl = new URL(sitemap);
      if (sitemapUrl) {
        const sitemapRes = await fetch(sitemapUrl);
        if (!sitemapRes.ok) console.error(`could not fetch sitemap ${sitemap}`);
        const sitemapRawXML = await sitemapRes.text();
        const parsedSitemap = (new DOMParser()).parseFromString(sitemapRawXML, 'application/xml');
        const locElements = parsedSitemap.querySelectorAll('loc');
        const urls = await locElements
          .values()
          .filter((e) => e.textContent.trim().match(pathVal))
          .map((e) => e.textContent.trim())
          .toArray();
        await urls.forEach(async (url) => {
          const urlRes = await fetch(url);
          if (!urlRes.ok) console.error(`could not fetch url ${url}`);
          const urlRawHTML = await urlRes.text();
          const parsedUrl = (new DOMParser()).parseFromString(urlRawHTML, 'text/html');
          const resultRow = document.createElement('tr');
          resultRow.classList.add('result');
          resultRow.innerHTML = `<td>${url}</td>`;
          const values = [];
          // eslint-disable-next-line consistent-return
          await selectors.forEach((selector) => {
            const elements = parsedUrl.querySelectorAll(selector);
            if (elements && elements.length === 0) return null;
            for (let i = 0; i < elements.length; i += 1) {
              if (elements[i].nodeName === 'A') {
                values.push(elements[i].getAttribute('href'));
                break;
              }
              if (elements[i].nodeType === 3) {
                values.push(elements[i].textContent);
                break;
              }
              values.push(elements[i].innerHTML);
            }
          });
          resultRow.innerHTML += `<td>${values.map((content) => `${content}`).join('\n')}</td>`;
          table.appendChild(resultRow);
        });
      } else {
        console.error(`Invalid URL ${sitemap}`);
      }
    });
  }
}

export default async function decorate(block) {
  block.innerHTML = `
  <div class="form">
    <form onsubmit="event.preventDefault();">
    <label for="sitemapUrls">Sitemap URLs: </label>
    <input 
      id="sitemapUrls"
      name="sitemapUrls"
      type="url"
      required="true"
      autocomplete="on" 
      placeholder="www.mydomain.com/sitemap_1.xml, www.mydomain.com/sitemap_2.xml" 
      title="Comma separated URLs of sitemaps"
      description="Please enter the sitemap urls of the site you want to crawl. If multiple, then comma separated."
    />
    <label for="path">Path: </label>
    <input 
      id="path"
      name="path"
      type="text"
      required="false"
      autocomplete="on" 
      placeholder="/path/to/crawl" 
      title="Path to Crawl (Recommended)"
      description="Please enter the path you want to limit the crawl to. (Recommended)"
    />
    <label for="selectors">Multiple CSS Selectors comma delimited</label>
    <input 
      id="selectors"
      name="selectors"
      type="textarea"
      required="true"
      autocomplete="on" 
      placeholder="head > meta['title'], body h1" 
      title="Please enter the content you want to extract via a comma-separated list of CSS selectors"
    />
    <button type="submit" id="formSubmit" name="formSubmit">Submit</button>
    </form>
  </div>
  <div class="results hidden">
    <h4 for="org">Results: </h4>
  </div>
  `;
  const submitButton = document.getElementById('formSubmit');
  submitButton.addEventListener('click', crawlAndExtract);
}
