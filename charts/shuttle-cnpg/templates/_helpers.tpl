{{- define "shuttle-cnpg.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "shuttle-cnpg.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- include "shuttle-cnpg.name" . -}}
{{- end -}}
{{- end -}}

{{- define "shuttle-cnpg.clusterName" -}}
{{- default (include "shuttle-cnpg.fullname" .) .Values.cluster.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "shuttle-cnpg.labels" -}}
app.kubernetes.io/name: {{ include "shuttle-cnpg.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}
