const regexpFull = /^.*github.com\/([a-zA-Z0-9_-]+)\/([a-zA-Z0-9_-]+)\/tree\/([a-zA-Z0-9_-]+)$/;
const regexpPartial = /^.*github.com\/([a-zA-Z0-9_-]+)\/([a-zA-Z0-9_-]+)$/;
const ghSubmit = document.getElementById('logSubmit');
const options = {
  valueNames: [
    'timestamp',
    'status',
    'method',
    'route',
    'path',
    'user',
    'errors',
    'duration',
  ],
  item: `<tr>
    <td class="timestamp"></td>
    <td class="status"></td>
    <td class="method"></td>
    <td class="route"></td>
    <td class="path"></td>
    <td class="user"></td>
    <td class="errors"></td>
    <td class="duration"></td>
  </tr>`,
};

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
  if (owner && repo && ref) return { owner, repo, ref };
  return {};
}

async function getLogs(githubUrl, from, to) {
  if (githubUrl === '') return [];
  const { owner, repo, ref } = processGithubUrl(githubUrl);
  if (owner && repo && ref) {
    const resp = await fetch(`https://admin.hlx.page/log/${owner}/${repo}/${ref}?from=${from}&to=${to}`, {
      credentials: 'include',
    });
    if (resp.status === 401) {
      addErrorMessage('401 Unauthorized. Please login to <a href="https://admin.hlx.page/login">https://admin.hlx.page/login</a> before viewing logs', ghSubmit);
      return [];
    }
    if (resp) {
      const logsJson = await resp.json();
      if (logsJson) {
        return logsJson.entries;
      }
    }
  }
  return [];
}

async function processForm() {
  // clear previous logs and any messages
  const messages = document.querySelectorAll('div.log');
  messages.forEach((message) => message.remove());
  const logs = document.querySelector('div#logs');
  if (logs) logs.remove();

  // Get the form values
  const ghUrl = document.getElementById('github-url').value;
  let fromDT = document.getElementById('from-date-time').value;
  let toDT = document.getElementById('to-date-time').value;
  // If from / to datetime is empty, default to last 24 hours
  if (fromDT === '') {
    // If empty or not selected, default to yesterday
    const dateObj = new Date();
    dateObj.setDate(dateObj.getDate() - 1);
    fromDT = dateObj.toISOString();
  }
  // If empty or not selected, default to now
  if (toDT === '') toDT = (new Date()).toISOString();
  if (regexpFull.test(ghUrl) || regexpPartial.test(ghUrl)) {
    // Get logs
    const values = await getLogs(ghUrl, fromDT, toDT);
    if (values && values.length > 0) {
      // githubUrlEl.classList.add('success');
      // update timestamps to be readableand create links for paths
      values.forEach((value) => {
        const dateObj = new Date(value.timestamp);
        value.timestamp = dateObj.toLocaleString();
        if (value.ref && value.repo && value.owner && value.path && value.route) {
          if (value.route === 'preview') {
            value.path = `<a href="https://${value.ref}--${value.repo}--${value.owner}.hlx.page${value.path}">${value.path}</a>`;
          }
          if (value.route === 'live') {
            value.path = `<a href="https://${value.ref}--${value.repo}--${value.owner}.hlx.live${value.path}">${value.path}</a>`;
          }
        }
        if (value.source === 'indexer') {
          value.route = 'indexer';
          value.path = value.changes;
        }
      });
      // Build list
      const table = `
        <div id="logs">
          <input type="search" class="search" placeholder="search">
          <!-- <button class="sort" data-sort="user">Sort by user</button>
          <button class="sort" data-sort="path">Sort by path</button> -->
          <table>
            <tbody class="list">
              <tr>
                <th>Timestamp</th>
                <th>Status</th>
                <th>Method</th>
                <th>Route</th>
                <th>Path</th>
                <th>User</th>
                <th>Errors</th>
                <th>Duration (ms)</th>
              </tr>
            </tbody>
          </table>
          <!-- <ul class="list"></ul> -->
        </div>
      `;
      document.body.querySelector('main').appendChild(document.createRange().createContextualFragment(table));
      // eslint-disable-next-line no-unused-vars, no-undef
      const logList = new List('logs', options, values);
    } else {
      // githubUrlEl.classList.add('error');
      addErrorMessage(`No logs found for ${ghUrl}`, ghSubmit);
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
  const calOptions = {
    input: true,
    settings: {
      selection: {
        time: 24,
      },
    },
    actions: {
      changeToInput(e, calendar, dates, time, hours, minutes) {
        if (dates[0]) {
          const selectedDT = new Date(`${dates[0]}T${hours}:${minutes}:00.000`);
          calendar.HTMLInputElement.value = selectedDT.toISOString();
        } else {
          calendar.HTMLInputElement.value = '';
        }
      },
    },
  };
  // eslint-disable-next-line no-undef
  const fromCalendar = new VanillaCalendar('#from-date-time', calOptions);
  // eslint-disable-next-line no-undef
  const toCalendar = new VanillaCalendar('#to-date-time', calOptions);
  fromCalendar.init();
  toCalendar.init();
  ghSubmit.addEventListener('click', processForm);
}

init();
