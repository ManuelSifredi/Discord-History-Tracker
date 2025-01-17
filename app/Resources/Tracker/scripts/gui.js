// noinspection FunctionWithInconsistentReturnsJS
const GUI = (function() {
	let controller = null;
	let settings = null;
	let trackingStyles = null;
	
	const stateChangedEvent = () => {
		if (settings) {
			settings.ui.cbAutoscroll.checked = SETTINGS.autoscroll;
			settings.ui.cbHidePreviewsWhileAutoscrolling.checked = SETTINGS.hidePreviewsWhileAutoscrolling;
			settings.ui.optsAfterFirstMsg[SETTINGS.afterFirstMsg].checked = true;
			settings.ui.optsAfterSavedMsg[SETTINGS.afterSavedMsg].checked = true;
			
			const autoscrollDisabled = !SETTINGS.autoscroll;
			settings.ui.cbHidePreviewsWhileAutoscrolling.disabled = autoscrollDisabled;
			Object.values(settings.ui.optsAfterFirstMsg).forEach(ele => ele.disabled = autoscrollDisabled);
			Object.values(settings.ui.optsAfterSavedMsg).forEach(ele => ele.disabled = autoscrollDisabled);
		}
	};
	
	return {
		showController() {
			if (controller) {
				return;
			}
			
			const html = `
<button id='dht-ctrl-close'>X</button>
<button id='dht-ctrl-settings'>Settings</button>
<button id='dht-ctrl-track'></button>
<p id='dht-ctrl-status'>Waiting</p>`;
			
			controller = {
				styles: DOM.createStyle(`/*[CSS-CONTROLLER]*/`),
				ele: DOM.createElement("div", document.body, "dht-ctrl", html)
			};
			
			controller.ui = {
				btnSettings: DOM.id("dht-ctrl-settings"),
				btnTrack: DOM.id("dht-ctrl-track"),
				btnClose: DOM.id("dht-ctrl-close"),
				textStatus: DOM.id("dht-ctrl-status")
			};
			
			controller.ui.btnSettings.addEventListener("click", () => {
				this.showSettings();
			});
			
			controller.ui.btnTrack.addEventListener("click", () => {
				const isTracking = !STATE.isTracking();
				STATE.setIsTracking(isTracking);
				
				if (!isTracking) {
					controller.ui.textStatus.innerText = "Stopped";
				}
			});
			
			controller.ui.btnClose.addEventListener("click", () => {
				this.hideController();
				this.deleteTrackingStyles();
				window.DHT_ON_UNLOAD.forEach(f => f());
				delete window.DHT_ON_UNLOAD;
				delete window.DHT_LOADED;
			});
			
			STATE.onTrackingStateChanged(isTracking => {
				controller.ui.btnTrack.innerText = isTracking ? "Pause Tracking" : "Start Tracking";
				controller.ui.btnSettings.disabled = isTracking;
			});
			
			SETTINGS.onSettingsChanged(stateChangedEvent);
			stateChangedEvent();
		},
		
		hideController() {
			if (controller) {
				DOM.removeElement(controller.ele);
				DOM.removeElement(controller.styles);
				controller = null;
			}
		},
		
		showSettings() {
			if (settings) {
				return;
			}
			
			const radio = (type, id, label) => "<label><input id='dht-cfg-" + type + "-" + id + "' name='dht-" + type + "' type='radio'> " + label + "</label><br>";
			const html = `
<label><input id='dht-cfg-autoscroll' type='checkbox'> Autoscroll</label><br>
<label><input id='dht-cfg-hide-previews-while-autoscrolling' type='checkbox'> Hide previews to improve browser performance</label><br>
<br>
<label>After reaching the first message in channel...</label><br>
${radio("afm", "nothing", "Continue Tracking")}
${radio("afm", "pause", "Pause Tracking")}
${radio("afm", "switch", "Switch to Next Channel")}
<br>
<label>After reaching a previously saved message...</label><br>
${radio("asm", "nothing", "Continue Tracking")}
${radio("asm", "pause", "Pause Tracking")}
${radio("asm", "switch", "Switch to Next Channel")}`;
			
			settings = {
				styles: DOM.createStyle(`/*[CSS-SETTINGS]*/`),
				overlay: DOM.createElement("div", document.body, "dht-cfg-overlay"),
				ele: DOM.createElement("div", document.body, "dht-cfg", html)
			};
			
			settings.overlay.addEventListener("click", () => {
				this.hideSettings();
			});
			
			settings.ui = {
				/** @type {HTMLInputElement} */ cbAutoscroll: DOM.id("dht-cfg-autoscroll"),
				/** @type {HTMLInputElement} */ cbHidePreviewsWhileAutoscrolling: DOM.id("dht-cfg-hide-previews-while-autoscrolling"),
				/** @type {Object.<number, HTMLInputElement>} */ optsAfterFirstMsg: {},
				/** @type {Object.<number, HTMLInputElement>} */ optsAfterSavedMsg: {}
			};
			
			settings.ui.optsAfterFirstMsg[CONSTANTS.AUTOSCROLL_ACTION_NOTHING] = DOM.id("dht-cfg-afm-nothing");
			settings.ui.optsAfterFirstMsg[CONSTANTS.AUTOSCROLL_ACTION_PAUSE] = DOM.id("dht-cfg-afm-pause");
			settings.ui.optsAfterFirstMsg[CONSTANTS.AUTOSCROLL_ACTION_SWITCH] = DOM.id("dht-cfg-afm-switch");
			
			settings.ui.optsAfterSavedMsg[CONSTANTS.AUTOSCROLL_ACTION_NOTHING] = DOM.id("dht-cfg-asm-nothing");
			settings.ui.optsAfterSavedMsg[CONSTANTS.AUTOSCROLL_ACTION_PAUSE] = DOM.id("dht-cfg-asm-pause");
			settings.ui.optsAfterSavedMsg[CONSTANTS.AUTOSCROLL_ACTION_SWITCH] = DOM.id("dht-cfg-asm-switch");
			
			settings.ui.cbAutoscroll.addEventListener("change", () => {
				SETTINGS.autoscroll = settings.ui.cbAutoscroll.checked;
			});
			
			settings.ui.cbHidePreviewsWhileAutoscrolling.addEventListener("change", () => {
				SETTINGS.hidePreviewsWhileAutoscrolling = settings.ui.cbHidePreviewsWhileAutoscrolling.checked;
			});
			
			Object.keys(settings.ui.optsAfterFirstMsg).forEach(key => {
				settings.ui.optsAfterFirstMsg[key].addEventListener("click", () => {
					SETTINGS.afterFirstMsg = key;
				});
			});
			
			Object.keys(settings.ui.optsAfterSavedMsg).forEach(key => {
				settings.ui.optsAfterSavedMsg[key].addEventListener("click", () => {
					SETTINGS.afterSavedMsg = key;
				});
			});
			
			stateChangedEvent();
		},
		
		hideSettings() {
			if (settings) {
				DOM.removeElement(settings.overlay);
				DOM.removeElement(settings.ele);
				DOM.removeElement(settings.styles);
				settings = null;
			}
		},
		
		setStatus(state) {
			if (controller) {
				controller.ui.textStatus.innerText = state;
			}
		},
		
		createTrackingStyles() {
			if (trackingStyles) {
				return;
			}
			
			let style = "";
			
			if (SETTINGS.hidePreviewsWhileAutoscrolling) {
				style += `div[id^="message-accessories-"] { display: none; }`;
			}
			
			if (style.length > 0) {
				trackingStyles = DOM.createStyle(style);
			}
		},
		
		deleteTrackingStyles() {
			if (trackingStyles) {
				DOM.removeElement(trackingStyles);
				trackingStyles = null;
			}
		}
	};
})();
