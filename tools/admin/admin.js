const regexpFull = /^.*github.com\/([a-zA-Z0-9_-]+)\/([a-zA-Z0-9_-]+)\/tree\/([a-zA-Z0-9_-]+)$/;
const regexpPartial = /^.*github.com\/([a-zA-Z0-9_-]+)\/([a-zA-Z0-9_-]+)$/;
const ghSubmit = document.getElementById('ghSubmit');
const configSubmit = document.getElementById('configSubmit');

function addErrorMessage(message, el) {
  const errorDiv = document.createElement('div');
  errorDiv.classList.add('log');
  errorDiv.classList.add('error');
  errorDiv.innerHTML = message;
  el.insertAdjacentElement('afterend', errorDiv);
}

function processGithubUrl(githubUrl) {
  let extractedUrl;
  if (githubUrl.includes('tree')) {
    extractedUrl = githubUrl.match(regexpFull);
  } else {
    extractedUrl = githubUrl.match(regexpPartial);
  }
  const owner = extractedUrl[1];
  const repo = extractedUrl[2];
  const ref = extractedUrl[3] || 'main';
  if (owner && repo && ref) {
    document.getElementById('org').value = owner;
    document.getElementById('site').value = repo;
    return { owner, repo, ref };
  }
  return {};
}

async function getConfig(githubUrl) {
  if (githubUrl === '') return [];
  const { owner, repo, ref } = processGithubUrl(githubUrl);
  if (owner && repo && ref) {
    const resp = await fetch(`https://admin.hlx.page/config/${owner}/sites/${repo}.json`, {
      credentials: 'include',
    });
    if (resp.status === 401) {
      addErrorMessage(`401 Unauthorized. Please login to <a href="https://admin.hlx.page/login">https://admin.hlx.page/login</a> before viewing configurations.
        You also need to have a role of admin to view site configurations`, ghSubmit);
      return [];
    }
    if (resp) {
      const configJson = await resp.json();
      if (configJson) {
        const configDiv = document.querySelector('div.config');
        if (configDiv.classList.contains('hidden')) configDiv.classList.remove('hidden');
        return configJson;
      }
    }
  }
  return [];
}
async function postConfig() {
  const config = document.getElementById('configTextArea').value;
  const organizationName = document.getElementById('org').value;
  const siteName = document.getElementById('site').value;
  const configJson = JSON.parse(config);
  if (siteName === '' || organizationName === '') return;
  const postConfigUrl = `https://admin.hlx.page/config/${organizationName}/sites/${siteName}.json`;
  const options = {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    credentials: 'include',
    body: configJson,
  };
  const resp = await fetch(postConfigUrl, options);
  if (resp.status === 401) {
    addErrorMessage(`401 Unauthorized. Please login to <a href="https://admin.hlx.page/login">https://admin.hlx.page/login</a> before saving configurations.
      You also need to have a role of admin to save site configurations`, ghSubmit);
    return;
  }
  if (resp) {
    const response = await resp.json();
    console.log(JSON.stringify(response));
  }
}

async function processForm() {
  // clear previous logs and any messages
  const messages = document.querySelectorAll('div.log');
  messages.forEach((message) => message.remove());
  const logs = document.querySelector('div#logs');
  if (logs) logs.remove();

  const configDiv = document.querySelector('div.config');
  if (!configDiv.classList.contains('hidden')) configDiv.classList.add('hidden');

  // Get the form values
  const ghUrl = document.getElementById('github-url').value;
  if (regexpFull.test(ghUrl) || regexpPartial.test(ghUrl)) {
    // Get logs
    const config = await getConfig(ghUrl);
    if (config) {
      // githubUrlEl.classList.add('success');
      const pretty = JSON.stringify(config, undefined, 4);
      document.getElementById('site-config').value = pretty;
    } else {
      // githubUrlEl.classList.add('error');
      addErrorMessage(`No configs found for ${ghUrl}`, ghSubmit);
    }
  } else {
    addErrorMessage(`The Github URL does not look right.
    Please enter the Github URL of the site you want to view logs for
    Example: https://www.github.com/amol-anand/tools/tree/main
    Example: www.github.com/amol-anand/tools
    Example: github.com/amol-anand/tools`, ghSubmit);
  }
}

function init() {
  ghSubmit.addEventListener('click', processForm);
  configSubmit.addEventListener('click', postConfig);
}

init();
