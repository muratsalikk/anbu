window.ANBU = window.ANBU || {};

window.ANBU.configureAceBase = function configureAceBase() {
  if (typeof ace === "undefined" || !ace.config) {
    return;
  }
  const basePath = window.ANBU_ACE_BASE || "/static/vendor/ace";
  ace.config.set("basePath", basePath);
};

window.ANBU.initAce = function initAce(editorId, value, options) {
  if (typeof ace === "undefined") {
    return null;
  }
  window.ANBU.configureAceBase();
  const node = document.getElementById(editorId);
  if (!node) {
    return null;
  }
  const cfg = options || {};
  const editor = ace.edit(editorId);
  editor.setTheme(cfg.theme || "ace/theme/github");
  editor.session.setMode(cfg.mode || "ace/mode/sql");
  editor.setShowPrintMargin(false);
  editor.session.setUseWrapMode(true);
  editor.setReadOnly(Boolean(cfg.readOnly));
  editor.setValue(value || "", -1);
  return editor;
};

window.ANBU.initAceReadOnly = function initAceReadOnly(editorId, sourceTextareaId) {
  const source = document.getElementById(sourceTextareaId);
  const value = source ? source.value : "";
  return window.ANBU.initAce(editorId, value, {
    mode: "ace/mode/sql",
    theme: "ace/theme/github",
    readOnly: true,
  });
};
