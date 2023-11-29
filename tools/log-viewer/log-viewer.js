const regexpFull = /^.*github.com\/([a-zA-Z0-9_-]+)\/([a-zA-Z0-9_-]+)\/tree\/([a-zA-Z0-9_-]+)$/;
const regexpPartial = /^.*github.com\/([a-zA-Z0-9_-]+)\/([a-zA-Z0-9_-]+)$/;
const ghSubmit = document.getElementById('githubsubmit');
const options = {
  valueNames: [
    'timestamp',
    'status',
    'method',
    'route',
    'path',
    'user',
    'site',
    'source',
    'contentBusId',
    'duration',
  ],
  item: `<tr>
    <td class="timestamp"></td>
    <td class="status"></td>
    <td class="method"></td>
    <td class="route"></td>
    <td class="path"></td>
    <td class="user"></td>
    <td class="site"></td>
    <td class="source"></td>
    <td class="contentBusId"></td>
    <td class="duration"></td>
  </tr>`,
};

function addErrorMessage(message, el) {
  const errorDiv = document.createElement('div');
  errorDiv.classList.add('log');
  errorDiv.classList.add('error');
  errorDiv.innerText = message;
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
  if (owner && repo && ref) return { owner, repo, ref };
  return {};
}

async function getLogs(githubUrl) {
  if (githubUrl === '') return [];
  const { owner, repo, ref } = processGithubUrl(githubUrl);
  if (owner && repo && ref) {
    const resp = await fetch(`https://admin.hlx.page/log/${owner}/${repo}/${ref}`, {
      credentials: 'include',
    });
    if (resp.status === 401) {
      addErrorMessage('401 Unauthorized. Please login to https://admin.hlx.page/login', ghSubmit);
      return [];
    }
    if (resp) {
      const logsJson = await resp.json();
      if (logsJson) {
        console.log(JSON.stringify(logsJson));
        return logsJson.entries;
      }
    }
  }
  return [];
}

async function saveGithubUrl() {
  // clear previous logs
  const logs = document.querySelectorAll('div.log');
  logs.forEach((log) => log.remove());
  const ghInput = document.getElementById('githuburl');
  const ghUrl = ghInput.value;
  if (regexpFull.test(ghUrl) || regexpPartial.test(ghUrl)) {
    const githubUrlEl = document.createElement('div');
    githubUrlEl.classList.add('log');
    githubUrlEl.classList.add('ghUrl');
    githubUrlEl.innerText = ghUrl;
    ghSubmit.insertAdjacentElement('afterend', githubUrlEl);
    // Get logs
    const values = await getLogs(ghUrl);
    if (values && values.length > 0) {
      githubUrlEl.classList.add('success');
    } else {
      githubUrlEl.classList.add('error');
      addErrorMessage(`No logs found for ${ghUrl}`, ghSubmit);
    }
    // Build list
    // eslint-disable-next-line no-unused-vars, no-undef
    const logList = new List('logs', options, values);
  } else {
    addErrorMessage(`The Github URL does not look right.
    Please enter the Github URL of the site you want to view logs for
    Example: https://www.github.com/amol-anand/tools/tree/main
    Example: www.github.com/amol-anand/tools
    Example: github.com/amol-anand/tools`, ghSubmit);
  }
}

ghSubmit.addEventListener('click', saveGithubUrl);
