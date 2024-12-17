DEBUG=1
REGISTRY=ghcr.io
OWNER=andylytical
REPO=access-tools


function latest_tag {
  [[ "$DEBUG" -eq 1 ]] && set -x
	# Replace <OWNER>, <REPO>, and <IMAGE> with the appropriate values for your container image.
  IMAGE=${REPO}
  URL="https://${REGISTRY}/v2/${OWNER}/${REPO}/${IMAGE}/tags/list"
	# local response=$(curl --silent "$URL")
	curl --silent "$URL"

	# Get the most recent tag by sorting the tags in reverse order and selecting the first one.
	# local latest_tag=$(echo "$response" | jq -r '.tags | sort -r | .[0]')
	# echo "$response" | jq -r '.tags | sort -r'

	# echo "$latest_tag"
}


function is_windows {
  rv=1
  [[ -n "$USERPROFILE" ]] && rv=0
  return $rv
}


[[ "$DEBUG" -eq 1 ]] && set -x

# tag=$(latest_tag)
# exit 1
tag=latest

action=''
src_home="$HOME"
if is_windows ; then
  action=winpty
  src_home="$USERPROFILE"
fi

$action docker run -it --pull always \
--mount type=bind,src="${src_home}",dst=/home \
--cap-add SYS_ADMIN \
--cap-add DAC_READ_SEARCH \
$REGISTRY/$OWNER/$REPO:$tag

