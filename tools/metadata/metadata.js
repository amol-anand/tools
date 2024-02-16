/* eslint-disable prefer-destructuring */
/* eslint-disable no-unused-vars */
/* eslint-disable no-undef */
const months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

function showAlert() {
  const alertBox = document.getElementById('customAlert');
  alertBox.style.display = 'block';
  setTimeout(() => {
    alertBox.style.display = 'none';
  }, 4000); // Dismiss after 4 seconds
}

function writeToClipboard(blob) {
  const data = [new ClipboardItem({ [blob.type]: blob })];
  navigator.clipboard.write(data);
}

async function populateTags() {
  // Replace with your JSON endpoint
  const tags = '/tags.json';
  const url = new URL(tags, window.location.origin);
  const resp = await fetch(url.toString());
  const response = await resp.json();
  if (response) {
    const { data } = response;
    const subjects = data.map((item) => [item['Subjects Text'], item['Subjects Value']]);
    const industries = data.map((item) => [item['Industries Text'], item['Industries Value']]);
    const selectSubjects = document.getElementById('dropdown-subjects');
    subjects.forEach((item) => {
      if (item) {
        const option = document.createElement('option');
        option.value = item[1];
        option.text = item[0];
        selectSubjects.appendChild(option);
      }
    });
    const selectIndustries = document.getElementById('dropdown-industries');
    industries.forEach((item) => {
      if (item) {
        const option = document.createElement('option');
        option.value = item[1];
        option.text = item[0];
        selectIndustries.appendChild(option);
      }
    });
  }
}

function processForm() {
  const publishDate = document.getElementById('publishDate').value || new Date().toISOString();
  const publishDateMetadata = publishDate.replace('T', ' ');
  const publishDateObj = new Date(publishDate);
  const publishDateFormatted = `${months[publishDateObj.getMonth()]} ${publishDateObj.getDate()}, ${publishDateObj.getFullYear()}`;
  const title = document.getElementById('title').value;
  const subTitle = document.getElementById('subtitle').value;
  const rawAbstract = document.querySelector('form div#abstract .ql-editor').innerHTML;
  const abstract = rawAbstract.replace(/<p>&nbsp;<\/p>/g, '');
  const rawBody = document.querySelector('form div#body .ql-editor').innerHTML;
  const body = rawBody.replace(/<p>&nbsp;<\/p>/g, '');
  const subjectsDropdown = document.getElementById('dropdown-subjects');
  const selectedSubjects = Array
    .from(subjectsDropdown.selectedOptions)
    .map((option) => option.value);
  const subjects = selectedSubjects.join(', ');
  const industriesDropdown = document.getElementById('dropdown-industries');
  const selectedIndustries = Array
    .from(industriesDropdown.selectedOptions)
    .map((option) => option.value);
  const industries = selectedIndustries.join(', ');
  // create the html to paste into the word doc
  const htmlToPaste = `
    ${publishDateFormatted || ''}
    <h1>${title || ''}</h1>
    <h6>${subTitle || ''}</h6>
    <br>
    ${abstract || ''}
    ---
    ${body || ''}
  
    <table border="1">
      <tr bgcolor="#f7caac">
        <td colspan="2">Metadata</td>
      </tr>
      <tr>
        <td>Template</td>
        <td>Article</td>
      </tr>
      <tr>
        <td>Published Date</td>
        <td>${publishDateMetadata || ''}</td>
      </tr>
      <tr>
        <td>Title</td>
        <td>${title || ''}</td>
      </tr>
      <tr>
        <td>Description</td>
        <td>${abstract || ''}</td>
      </tr>
      <tr>
        <td>Abstract</td>
        <td>${abstract || ''}</td>
      </tr>
      <tr>
        <td>Subjects</td>
        <td>${subjects || ''}</td>
      </tr>
      <tr>
        <td>Industries</td>
        <td>${industries || ''}</td>
      </tr>
      <tr>
        <td>Keywords</td>
        <td></td>
      </tr>
    </table>
  `;
  writeToClipboard(new Blob([htmlToPaste], { type: 'text/html' }));
  showAlert();
}

async function init() {
  await populateTags();
  const rteOptions = {
    modules: {
      toolbar: [
        ['bold', 'italic', 'underline'],
        ['link'],
        ['clean'],
      ],
    },
    theme: 'snow',
  };
  const abstractContainer = document.querySelector('form div#abstract');
  const abstractEditor = await new Quill(abstractContainer, rteOptions);
  const bodyContainer = document.querySelector('form div#body');
  const bodyEditor = await new Quill(bodyContainer, rteOptions);
  const copyButton = document.getElementById('copyToClipboard');
  copyButton.addEventListener('click', () => {
    processForm();
  });
}

await init();
