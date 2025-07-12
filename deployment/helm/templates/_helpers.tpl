{{/*
Expand the name of the chart.
*/}}
{{- define "newmind-ai.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "newmind-ai.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "newmind-ai.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "newmind-ai.labels" -}}
helm.sh/chart: {{ include "newmind-ai.chart" . }}
{{ include "newmind-ai.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "newmind-ai.selectorLabels" -}}
app.kubernetes.io/name: {{ include "newmind-ai.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "newmind-ai.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "newmind-ai.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the config map
*/}}
{{- define "newmind-ai.configMapName" -}}
{{- printf "%s-config" (include "newmind-ai.fullname" .) }}
{{- end }}

{{/*
Create the name of the secret
*/}}
{{- define "newmind-ai.secretName" -}}
{{- printf "%s-secret" (include "newmind-ai.fullname" .) }}
{{- end }}

{{/*
Create the name of the persistent volume claim
*/}}
{{- define "newmind-ai.pvcName" -}}
{{- printf "%s-pvc" (include "newmind-ai.fullname" .) }}
{{- end }}

{{/*
Create the name of the service monitor
*/}}
{{- define "newmind-ai.serviceMonitorName" -}}
{{- printf "%s-servicemonitor" (include "newmind-ai.fullname" .) }}
{{- end }}

{{/*
Create the name of the pod disruption budget
*/}}
{{- define "newmind-ai.pdbName" -}}
{{- printf "%s-pdb" (include "newmind-ai.fullname" .) }}
{{- end }}

{{/*
Create the name of the horizontal pod autoscaler
*/}}
{{- define "newmind-ai.hpaName" -}}
{{- printf "%s-hpa" (include "newmind-ai.fullname" .) }}
{{- end }}

{{/*
Create the name of the network policy
*/}}
{{- define "newmind-ai.networkPolicyName" -}}
{{- printf "%s-netpol" (include "newmind-ai.fullname" .) }}
{{- end }}