{{- define "shuttle-monitoring-resources.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "shuttle-monitoring-resources.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- include "shuttle-monitoring-resources.name" . -}}
{{- end -}}
{{- end -}}

{{- define "shuttle-monitoring-resources.labels" -}}
app.kubernetes.io/name: {{ include "shuttle-monitoring-resources.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}
