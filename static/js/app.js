const { createApp } = Vue;

async function api(path, options = {}) {
  const res = await fetch(path, options);
  let data = {};
  try {
    data = await res.json();
  } catch (e) {
    data = {};
  }
  if (!res.ok) {
    const message = data.error || data.detail || "请求失败";
    const err = new Error(message);
    err.data = data;
    err.status = res.status;
    throw err;
  }
  return data;
}

function post(path, body) {
  return api(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
}

createApp({
  data() {
    return {
      me: "",
      items: [],
      current: null,
      mappings: [],
      selectedMapping: null,
      policy: null,
      policyChoice: "",
      policyNote: "",
      diffFrom: 1,
      diffTo: 1,
      switchTarget: "",
      busy: false,
      msg: "",
      newMapping: { name: "", code: "", columns: "", code_column: "" },
      newVersion: { columns: "", code_column: "", base_version: "" },
    };
  },
  mounted() {
    this.boot();
  },
  computed: {
    diff() {
      if (!this.current) return null;
      const from = (this.current.versions || []).find((v) => v.version_no === Number(this.diffFrom));
      const to = (this.current.versions || []).find((v) => v.version_no === Number(this.diffTo));
      if (!from || !to) return null;
      return this.compareVersions(from, to);
    },
  },
  methods: {
    async boot() {
      try {
        const me = await api("/api/me");
        this.me = me.user;
      } catch (e) {
        location.href = "/login";
        return;
      }
      await this.refresh();
      await this.loadMappings();
      if (this.items.length) await this.open(this.items[0].id);
    },
    tagClass(status) {
      if (status === "已落库" || status === "已确认") return "ok";
      if (status === "已抽取待加载") return "warn";
      return "";
    },
    async refresh() {
      try {
        const data = await api("/api/overview");
        this.items = data.items || [];
        this.policy = data.policy || null;
      } catch (e) {
        this.flash(e.message);
      }
    },
    async loadMappings() {
      try {
        const data = await api("/api/mappings");
        this.mappings = data.items || [];
        if (this.selectedMapping) {
          const again = this.mappings.find((m) => m.id === this.selectedMapping.id);
          if (again) this.selectedMapping = again;
        }
      } catch (e) {
        this.flash(e.message);
      }
    },
    async open(id) {
      try {
        const detail = await api(`/api/pipelines/${id}`);
        this.current = detail;
        this.msg = "";
        if (detail.versions && detail.versions.length) {
          const bound = detail.bound_version_no || detail.versions[0].version_no;
          this.diffFrom = bound;
          this.diffTo = detail.versions[detail.versions.length - 1].version_no;
        }
        this.switchTarget = "";
      } catch (e) {
        this.flash(e.message);
      }
    },
    compareVersions(from, to) {
      const added = to.columns.filter((c) => !from.columns.includes(c));
      const removed = from.columns.filter((c) => !to.columns.includes(c));
      const reordered = to.columns.filter(
        (c, i) =>
          from.columns.includes(c) &&
          from.columns.indexOf(c) !== i &&
          c !== to.code_column
      );
      return {
        added,
        removed,
        reordered,
        codeFrom: from.code_column || "缺失",
        codeTo: to.code_column || "缺失",
      };
    },
    async extract() {
      if (!this.current) return;
      await this.run(`/api/pipelines/${this.current.id}/extract`, "抽取完成，可继续落库");
    },
    async load() {
      if (!this.current) return;
      await this.run(`/api/pipelines/${this.current.id}/load`, "落库完成");
    },
    async withdraw() {
      if (!this.current) return;
      await this.run(`/api/pipelines/${this.current.id}/withdraw`, "整批撤回完成");
    },
    async confirm() {
      if (!this.current) return;
      await this.run(`/api/pipelines/${this.current.id}/confirm`, "批次确认，版本钉死");
    },
    async switchVersion() {
      if (!this.current || !this.switchTarget) return;
      this.busy = true;
      this.msg = "";
      try {
        const data = await post(`/api/pipelines/${this.current.id}/switch`, {
          to_version_no: Number(this.switchTarget),
        });
        const extra = data.withdrawn_rows
          ? `，已整批撤回 ${data.withdrawn_rows} 行`
          : "";
        this.msg = `已切换到 v${data.to_version_no}${extra}，请按新版本重转换`;
        if (data.policy === "先撤再落" && !data.withdrawn_rows) {
          this.msg += "（当日策略：先撤再落）";
        }
      } catch (e) {
        this.msg = e.message;
      } finally {
        this.busy = false;
      }
      await this.reloadAll();
    },
    async run(path, okMessage, method = "POST") {
      this.busy = true;
      this.msg = "";
      try {
        await api(path, { method });
        this.msg = okMessage;
      } catch (e) {
        this.msg = e.message;
      } finally {
        this.busy = false;
      }
      await this.reloadAll();
    },
    async reloadAll() {
      await this.refresh();
      if (this.current) await this.open(this.current.id);
      await this.loadMappings();
    },
    async selectMapping(id) {
      try {
        const data = await api(`/api/mappings/${id}`);
        this.selectedMapping = data;
        const latest = data.versions[data.versions.length - 1];
        if (latest) {
          this.newVersion.columns = latest.columns.join(", ");
          this.newVersion.code_column = latest.code_column || "";
          this.newVersion.base_version = String(latest.version_no);
        }
      } catch (e) {
        this.flash(e.message);
      }
    },
    async createMapping() {
      this.busy = true;
      try {
        const data = await post("/api/mappings", this.newMapping);
        this.msg = `映射已建立，版本 ${data.mapping.latest_version.version_label}`;
        this.newMapping = { name: "", code: "", columns: "", code_column: "" };
        await this.loadMappings();
      } catch (e) {
        this.msg = e.message;
      } finally {
        this.busy = false;
      }
    },
    async saveVersion() {
      if (!this.selectedMapping) return;
      this.busy = true;
      try {
        const body = {
          columns: this.newVersion.columns,
          code_column: this.newVersion.code_column,
        };
        if (this.newVersion.base_version) body.base_version = Number(this.newVersion.base_version);
        const data = await post(`/api/mappings/${this.selectedMapping.id}/versions`, body);
        this.msg = data.created
          ? `已保存 ${data.version_label}`
          : `内容相同，复用 ${data.version_label}`;
        await this.reloadAll();
      } catch (e) {
        this.msg = e.message;
      } finally {
        this.busy = false;
      }
    },
    async publishVersion(versionNo) {
      if (!this.selectedMapping) return;
      this.busy = true;
      try {
        await post(`/api/mappings/${this.selectedMapping.id}/versions/${versionNo}/publish`, {});
        this.msg = `v${versionNo} 已发布`;
        await this.reloadAll();
      } catch (e) {
        this.msg = e.message;
      } finally {
        this.busy = false;
      }
    },
    async savePolicy() {
      if (!this.policyChoice) return;
      this.busy = true;
      try {
        await post("/api/switch-policy", {
          policy: this.policyChoice,
          note: this.policyNote,
        });
        this.msg = "今日换版策略已锁定";
        await this.reloadAll();
      } catch (e) {
        this.msg = e.message;
      } finally {
        this.busy = false;
      }
    },
    flash(message) {
      this.msg = message;
    },
    async logout() {
      await fetch("/api/logout", { method: "POST" });
      location.href = "/login";
    },
  },
}).mount("#app");
