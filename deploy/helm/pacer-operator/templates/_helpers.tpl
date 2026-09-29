{{- define "pacer-operator.name" -}}
{{- .Chart.Name -}}
{{- end -}}

{{- define "pacer-operator.fullname" -}}
{{- .Release.Name -}}
{{- end -}}

{{- define "pacer-operator.labels" -}}
app.kubernetes.io/name: {{ include "pacer-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "pacer-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pacer-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "pacer-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- .Values.serviceAccount.name | default (include "pacer-operator.fullname" .) -}}
{{- else -}}
{{- .Values.serviceAccount.name | required "serviceAccount.name is required when serviceAccount.create is false" -}}
{{- end -}}
{{- end -}}

