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
    'site',
    'source',
    'duration',
    'contentBusId',
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
    <td class="duration"></td>
    <td class="contentBusId"></td>
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

async function getLogs(githubUrl, from, to) {
  if (githubUrl === '') return [];
  const { owner, repo, ref } = processGithubUrl(githubUrl);
  if (owner && repo && ref) {
    const resp = await fetch(`https://admin.hlx.page/log/${owner}/${repo}/${ref}?from=${from}&to=${to}`, {
      credentials: 'include',
    });
    if (resp.status === 401) {
      addErrorMessage('401 Unauthorized. Please login to https://admin.hlx.page/login', ghSubmit);
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
  // clear previous logs
  const logs = document.querySelectorAll('div.log');
  logs.forEach((log) => log.remove());

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
    console.log(`FROM DATE: current time: ${dateObj.toString()}, UTC time: ${fromDT}`);
  }
  // If empty or not selected, default to now
  if (toDT === '') toDT = (new Date()).toISOString();
  console.log(`TO DATE: current time: ${(new Date()).toString()}, UTC time: ${toDT}`);
  if (regexpFull.test(ghUrl) || regexpPartial.test(ghUrl)) {
    // const githubUrlEl = document.createElement('div');
    // githubUrlEl.classList.add('log');
    // githubUrlEl.classList.add('ghUrl');
    // githubUrlEl.innerText = ghUrl;
    // ghSubmit.insertAdjacentElement('afterend', githubUrlEl);
    // Get logs
    const values = await getLogs(ghUrl, fromDT, toDT);
    if (values && values.length > 0) {
      // githubUrlEl.classList.add('success');
      // update timestamps
      values.forEach((value) => {
        const dateObj = new Date(value.timestamp);
        value.timestamp = dateObj.toLocaleString();
      });
      // Build list
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
          // 1995-12-17T03:24:00
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
